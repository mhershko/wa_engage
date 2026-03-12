"""JimmyHandler – core message routing for the Jimmy bot.

Every message goes through the LLM for a natural conversational response.
The handler manages:
- Onboarding (first contact)
- Conversation via LLM (all subsequent messages)
- Escalation to admin WhatsApp group when needed
- Admin commands (/approve, /reset_leader)
"""

import asyncio
import logging
import re
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from cachetools import TTLCache
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from config import Settings
from jimmy.brain import Intent, JimmyBrain, get_recent_timing_events
from jimmy.notion_client import LeaderRecord, NotionClient
from jimmy import templates as T
from models.answer_review import AnswerReview
from models.escalated_question import EscalatedQuestion
from models.leader_state import LeaderState, OnboardingStage
from models.message import Message
from whatsapp import SendMessageRequest, WhatsAppClient
from whatsapp.jid import normalize_jid

logger = logging.getLogger(__name__)
_clarification_cache = TTLCache(maxsize=2000, ttl=10 * 60)
_clarification_lock = asyncio.Lock()
_admin_log_level = "debug"  # options: debug, unclear_only
_admin_log_level_lock = asyncio.Lock()
_page_choice_cache = TTLCache(maxsize=500, ttl=20 * 60)
_page_choice_lock = asyncio.Lock()


def _phone_from_jid(jid: str) -> str:
    """Extract the phone digits from a WhatsApp JID."""
    return re.sub(r"@.*$", "", jid)


class JimmyHandler:
    def __init__(
        self,
        session: AsyncSession,
        whatsapp: WhatsAppClient,
        settings: Settings,
        notion: NotionClient,
        brain: JimmyBrain,
    ):
        self._session = session
        self._wa = whatsapp
        self._settings = settings
        self._notion = notion
        self._brain = brain
        self._slow_response_notice_after_sec = 25

    # ------------------------------------------------------------------
    # Public entry points
    # ------------------------------------------------------------------

    async def handle_dm(self, sender_jid: str, message_text: str) -> None:
        """Handle a direct message to Jimmy from a leader/admin/unknown."""
        phone = _phone_from_jid(normalize_jid(sender_jid))
        leader = await self._notion.get_leader_by_phone(
            phone, self._settings.notion_leaders_db_id
        )

        state = await self._get_or_create_state(phone)

        if leader is None:
            await self._send_dm(sender_jid, T.unknown_user())
            return

        if state.onboarding_stage == OnboardingStage.NEW:
            await self._onboard(sender_jid, leader, state)
            return

        latest_gender = self._gender_from_leader(leader)
        if latest_gender and state.preferred_gender != latest_gender:
            state.preferred_gender = latest_gender
            state.updated_at = datetime.now(timezone.utc)
            self._session.add(state)
            await self._session.flush()
        elif not state.preferred_gender:
            state.preferred_gender = latest_gender
            state.updated_at = datetime.now(timezone.utc)
            self._session.add(state)
            await self._session.flush()

        await self._converse(sender_jid, message_text, leader, state)

    async def handle_group_add(
        self, group_jid: str, group_name: str, adder_jid: str | None
    ) -> None:
        """Bot was added to a WhatsApp group."""
        phone = _phone_from_jid(normalize_jid(adder_jid)) if adder_jid else "unknown"
        state = await self._get_state_by_phone(phone)

        if state:
            state.group_id = group_jid
            state.group_name = group_name
            state.group_approved = False
            state.updated_at = datetime.now(timezone.utc)
            self._session.add(state)
            await self._session.flush()

        await self._send_to_admin_group(
            T.GROUP_BINDING_REQUEST.format(
                whatsapp_group_name=group_name,
                leader_phone=phone,
            )
        )

    async def handle_group_message(
        self, group_jid: str, sender_jid: str, message_text: str
    ) -> None:
        """Handle a message in a group where Jimmy is present.

        Jimmy never writes in participant groups. Only parse admin commands
        if this is the admin group.
        """
        if group_jid != self._settings.admin_whatsapp_group_id:
            return

        await self._handle_admin_command(sender_jid, message_text)

    # ------------------------------------------------------------------
    # Onboarding
    # ------------------------------------------------------------------

    async def _onboard(
        self, sender_jid: str, leader: LeaderRecord, state: LeaderState
    ) -> None:
        await self._send_dm(sender_jid, T.welcome_message(leader))

        state.onboarding_stage = OnboardingStage.ONBOARDED
        state.preferred_gender = "masculine" if leader.is_masculine else "feminine"
        state.updated_at = datetime.now(timezone.utc)
        self._session.add(state)
        await self._session.flush()

    # ------------------------------------------------------------------
    # Conversation (all post-onboarding messages go through the LLM)
    # ------------------------------------------------------------------

    async def _converse(
        self,
        sender_jid: str,
        message_text: str,
        leader: LeaderRecord,
        state: LeaderState,
    ) -> None:
        total_started = time.perf_counter()
        sender_key = normalize_jid(sender_jid)
        if await self._handle_explicit_page_answer_request(
            sender_jid=sender_jid,
            message_text=message_text,
            leader=leader,
            state=state,
        ):
            return
        if await self._handle_pending_page_choice(
            sender_jid=sender_jid,
            sender_key=sender_key,
            message_text=message_text,
            leader=leader,
            state=state,
        ):
            return

        classify_started = time.perf_counter()
        classification = await self._brain.classify_intent(message_text)
        logger.info(
            "jimmy_timing %s",
            {
                "stage": "handler_classify",
                "latency_ms": round((time.perf_counter() - classify_started) * 1000, 2),
                "intent": classification.intent.value,
            },
        )
        clarification_allowed = await self._clarification_allowed(sender_key)
        admin_log_level = await self._get_admin_log_level()
        leader_gender = self._effective_leader_gender(leader, state)
        is_debug = admin_log_level == "debug"
        knowledge_intents = {Intent.ADMIN_LOGISTICS, Intent.META_PROGRAM, Intent.UNKNOWN}
        local_corrections = await self._get_local_corrections(message_text)

        respond_started = time.perf_counter()
        response_task = asyncio.create_task(
            self._brain.respond(
                message_text=message_text,
                leader_name=leader.name,
                leader_gender=leader_gender,
                leader_role=leader.role or "",
                leader_group=leader.group_name if leader.has_group else None,
                intent=classification.intent,
                event_type=classification.event_type,
                clarification_allowed=clarification_allowed,
                local_corrections=local_corrections,
            )
        )

        try:
            result = await asyncio.wait_for(
                asyncio.shield(response_task),
                timeout=self._slow_response_notice_after_sec,
            )
        except TimeoutError:
            await self._send_dm(
                sender_jid, T.long_processing_notice(leader, leader_gender=leader_gender)
            )
            result = await response_task
        logger.info(
            "jimmy_timing %s",
            {
                "stage": "handler_respond",
                "latency_ms": round((time.perf_counter() - respond_started) * 1000, 2),
                "needs_clarification": result.needs_clarification,
                "should_escalate": result.should_escalate,
            },
        )

        uncertain_for_menu = (
            classification.intent in knowledge_intents
            and (
                result.needs_clarification
                or result.is_grounded is False
                or result.should_escalate
            )
        )
        if uncertain_for_menu:
            offered_page_choice, _ = await asyncio.gather(
                self._offer_page_choice(
                    sender_jid=sender_jid,
                    sender_key=sender_key,
                    question=message_text,
                    leader=leader,
                ),
                self._notify_uncertain_source_selection(
                    leader=leader,
                    question=message_text,
                    intent=classification.intent.value,
                    uncertainty_reason=result.uncertainty_reason,
                ),
            )
            if offered_page_choice:
                if is_debug:
                    await self._send_debug_trace(
                        leader=leader,
                        question=message_text,
                        answer="הוצעה בחירת עמוד/ים מהאינדקס בעקבות חוסר ודאות.",
                        intent=classification.intent.value,
                        needs_clarification=result.needs_clarification,
                        should_escalate=result.should_escalate,
                        is_grounded=result.is_grounded,
                        source_count=result.source_count,
                        flow_mode="menu_fallback",
                        uncertainty_reason=result.uncertainty_reason,
                    )
                if result.needs_clarification:
                    await self._mark_clarification_asked(sender_key)
                return

        if result.needs_clarification:
            offered_page_choice = await self._offer_page_choice(
                sender_jid=sender_jid,
                sender_key=sender_key,
                question=message_text,
                leader=leader,
            )
            if offered_page_choice:
                if is_debug:
                    await self._send_debug_trace(
                        leader=leader,
                        question=message_text,
                        answer="הוצעה בחירת עמוד ידע מהרשימה לפני הסלמה.",
                        intent=classification.intent.value,
                        needs_clarification=True,
                        should_escalate=False,
                        is_grounded=result.is_grounded,
                        source_count=result.source_count,
                        flow_mode="menu_fallback",
                        uncertainty_reason=result.uncertainty_reason,
                    )
                await self._mark_clarification_asked(sender_key)
                return

            bot_answer = result.clarification_question or result.response
            await self._send_dm(
                sender_jid,
                bot_answer,
            )
            review_id = await self._create_answer_review(
                leader=leader,
                question=message_text,
                bot_answer=bot_answer,
                intent_type=f"{result.intent.value}_CLARIFICATION",
                leader_jid=normalize_jid(sender_jid),
            )
            if is_debug:
                await self._send_debug_trace(
                    leader=leader,
                    question=message_text,
                    answer=bot_answer,
                    intent=classification.intent.value,
                    needs_clarification=True,
                    should_escalate=True,
                    review_id=review_id,
                    is_grounded=result.is_grounded,
                    source_count=result.source_count,
                    flow_mode="clarification",
                    uncertainty_reason=result.uncertainty_reason,
                )
            await self._send_to_admin_group(
                "\n".join(
                    [
                        T.ADMIN_ESCALATION_LOGISTICS.format(
                            movil="מוביל" if leader_gender == "masculine" else "מובילה",
                            leader_name=leader.name,
                            leader_group=leader.group_name or "—",
                            leader_phone=leader.phone,
                            original_question=message_text,
                            intent_type=f"{result.intent.value}_CLARIFICATION",
                        ),
                        "",
                        f"Review ID: {review_id}",
                        "תשובת ג׳ימי:",
                        bot_answer,
                        "לאישור: /review_ok <review_id>",
                        "לתיקון טיוטה: /review_fix <review_id> <תשובה מתוקנת>",
                        "לתיקון לפי עמוד/ים: /review_fix_page <review_id> <page refs>",
                        "לשליחת טיוטה: /review_send <review_id>",
                    ]
                )
            )
            await self._store_escalation(
                leader, message_text, f"{result.intent.value}_CLARIFICATION"
            )
            await self._mark_clarification_asked(sender_key)
            return

        await self._clear_clarification_state(sender_key)
        await self._send_dm(sender_jid, result.response)
        review_id: str | None = None
        if is_debug or result.should_escalate:
            review_id = await self._create_answer_review(
                leader=leader,
                question=message_text,
                bot_answer=result.response,
                intent_type=result.intent.value,
                leader_jid=normalize_jid(sender_jid),
            )
        if is_debug:
            await self._send_debug_trace(
                leader=leader,
                question=message_text,
                answer=result.response,
                intent=classification.intent.value,
                needs_clarification=result.needs_clarification,
                should_escalate=result.should_escalate,
                review_id=review_id,
                is_grounded=result.is_grounded,
                source_count=result.source_count,
                flow_mode="direct_answer",
                uncertainty_reason=result.uncertainty_reason,
            )

        if result.should_escalate:
            await self._send_to_admin_group(
                "\n".join(
                    [
                        T.ADMIN_ESCALATION_LOGISTICS.format(
                            movil="מוביל" if leader_gender == "masculine" else "מובילה",
                            leader_name=leader.name,
                            leader_group=leader.group_name or "—",
                            leader_phone=leader.phone,
                            original_question=message_text,
                            intent_type=result.intent.value,
                        ),
                        "",
                        f"Review ID: {review_id or '—'}",
                        "תשובת ג׳ימי:",
                        result.response,
                        "לאישור: /review_ok <review_id>",
                        "לתיקון טיוטה: /review_fix <review_id> <תשובה מתוקנת>",
                        "לתיקון לפי עמוד/ים: /review_fix_page <review_id> <page refs>",
                        "לשליחת טיוטה: /review_send <review_id>",
                    ]
                )
            )
            await self._store_escalation(
                leader, message_text, result.intent.value
            )
        logger.info(
            "jimmy_timing %s",
            {
                "stage": "handler_total",
                "latency_ms": round((time.perf_counter() - total_started) * 1000, 2),
            },
        )

    # ------------------------------------------------------------------
    # Admin commands
    # ------------------------------------------------------------------

    async def _handle_admin_command(self, sender_jid: str, message_text: str) -> None:
        text = message_text.strip()

        if text == "/help":
            await self._cmd_help()
            return

        review_ok_match = re.match(r"^/review_ok\s+([A-Za-z0-9_-]+)\s*$", text)
        if review_ok_match:
            await self._cmd_review_ok(sender_jid, review_ok_match.group(1))
            return

        review_send_match = re.match(r"^/review_send\s+([A-Za-z0-9_-]+)\s*$", text)
        if review_send_match:
            await self._cmd_review_send(sender_jid, review_send_match.group(1))
            return

        review_update_match = re.match(
            r"^/review_update\s+([A-Za-z0-9_-]+)\s+(.+)$",
            text,
            re.DOTALL,
        )
        if review_update_match:
            await self._cmd_review_update(
                sender_jid,
                review_update_match.group(1),
                review_update_match.group(2),
            )
            return

        review_fix_page_match = re.match(
            r"^/review_fix_page\s+([A-Za-z0-9_-]+)\s+(.+)$",
            text,
            re.DOTALL,
        )
        if review_fix_page_match:
            await self._cmd_review_fix_page(
                sender_jid,
                review_fix_page_match.group(1),
                review_fix_page_match.group(2),
            )
            return

        review_fix_match = re.match(r"^/review_fix\s+([A-Za-z0-9_-]+)\s+(.+)$", text, re.DOTALL)
        if review_fix_match:
            await self._cmd_review_fix(
                sender_jid,
                review_fix_match.group(1),
                review_fix_match.group(2),
            )
            return

        log_mode_match = re.match(r"^/log_mode(?:\s+(\S+))?$", text)
        if log_mode_match:
            await self._cmd_log_mode(log_mode_match.group(1))
            return

        approve_match = re.match(r"^/approve\s+(\S+)", text)
        if approve_match:
            await self._cmd_approve(approve_match.group(1))
            return

        reset_match = re.match(r"^/reset_leader\s+(\S+)", text)
        if reset_match:
            await self._cmd_reset_leader(reset_match.group(1))
            return

        usage_match = re.match(r"^/usage_report(?:\s+(\d+))?$", text)
        if usage_match:
            days = int(usage_match.group(1) or "7")
            await self._cmd_usage_report(days)
            return

        sync_match = re.match(r"^/sync_leaders(?:\s+(\S+))?$", text)
        if sync_match:
            await self._cmd_sync_leaders(sync_match.group(1))
            return

        if text == "/refresh_notion":
            await self._cmd_refresh_notion()
            return

        clear_local_cache_match = re.match(
            r"^/clear_local_cache(?:\s+(.+))?$",
            text,
            re.DOTALL,
        )
        if clear_local_cache_match:
            await self._cmd_clear_local_cache(clear_local_cache_match.group(1))
            return

        perf_match = re.match(r"^/perf_last(?:\s+(\d+))?$", text)
        if perf_match:
            count = int(perf_match.group(1) or "20")
            await self._cmd_perf_last(count)
            return

        if text == "/refresh_purposes":
            await self._cmd_refresh_purposes()
            return

        set_purpose_match = re.match(
            r"^/set_purpose\s+(.+?)\s*\|\s*(.+)$", text, re.DOTALL
        )
        if set_purpose_match:
            await self._cmd_set_purpose(
                set_purpose_match.group(1).strip(),
                set_purpose_match.group(2).strip(),
            )
            return

    async def _cmd_approve(self, raw_phone: str) -> None:
        phone = re.sub(r"\D", "", raw_phone)
        state = await self._get_state_by_phone(phone)
        if not state:
            await self._send_to_admin_group(f"לא נמצא מוביל עם הטלפון {phone}")
            return

        state.group_approved = True
        state.updated_at = datetime.now(timezone.utc)
        self._session.add(state)
        await self._session.flush()

        leader_jid = f"{phone}@s.whatsapp.net"
        await self._send_dm(leader_jid, T.GROUP_APPROVED)

    async def _cmd_reset_leader(self, raw_phone: str) -> None:
        phone = re.sub(r"\D", "", raw_phone)
        state = await self._get_state_by_phone(phone)
        if not state:
            await self._send_to_admin_group(f"לא נמצא מוביל עם הטלפון {phone}")
            return

        state.onboarding_stage = OnboardingStage.NEW
        state.group_id = None
        state.group_name = None
        state.group_approved = False
        state.preferred_gender = None
        state.updated_at = datetime.now(timezone.utc)
        self._session.add(state)
        await self._session.flush()

        leader_jid = f"{phone}@s.whatsapp.net"
        leader = await self._notion.get_leader_by_phone(
            phone, self._settings.notion_leaders_db_id
        )
        await self._send_dm(leader_jid, T.leader_reset(leader))

    async def _cmd_usage_report(self, days: int) -> None:
        days = max(1, min(days, 90))
        since = datetime.now(timezone.utc) - timedelta(days=days)

        leaders = await self._notion.get_all_leaders(self._settings.notion_leaders_db_id)
        leader_by_jid = {f"{leader.phone}@s.whatsapp.net": leader for leader in leaders}

        stmt = select(Message).where(Message.timestamp >= since, Message.group_jid.is_(None))
        result = await self._session.exec(stmt)
        messages = result.all()

        msg_count_by_jid: dict[str, int] = defaultdict(int)
        for msg in messages:
            sender = normalize_jid(msg.sender_jid)
            if sender in leader_by_jid and (msg.text or "").strip():
                msg_count_by_jid[sender] += 1

        total_leaders = len(leaders)
        active_leaders = sum(1 for jid in leader_by_jid if msg_count_by_jid.get(jid, 0) > 0)
        total_messages = sum(msg_count_by_jid.values())

        ranked = sorted(
            (
                (leader_by_jid[jid], count)
                for jid, count in msg_count_by_jid.items()
                if count > 0
            ),
            key=lambda item: item[1],
            reverse=True,
        )[:10]

        lines = [
            f"📊 דוח שימוש בג׳ימי ({days} ימים אחרונים)",
            f"סה״כ מובילים במערכת: {total_leaders}",
            f"מובילים פעילים: {active_leaders}",
            f"סה״כ הודעות למערכת: {total_messages}",
            "",
            "Top 10 שימוש:",
        ]
        if not ranked:
            lines.append("אין פעילות בתקופה הזו.")
        else:
            for idx, (leader, count) in enumerate(ranked, start=1):
                group_name = leader.group_name or "ללא קבוצה"
                lines.append(f"{idx}. {leader.name} ({group_name}) — {count} הודעות")

        await self._send_to_admin_group("\n".join(lines))

    async def _cmd_log_mode(self, mode: str | None) -> None:
        if mode is None:
            current = await self._get_admin_log_level()
            await self._send_to_admin_group(
                "רמת לוג נוכחית: "
                f"{current}\n"
                "רמות זמינות:\n"
                "- debug: כל הודעה ממובילים\n"
                "- unclear_only: רק שאלות ללא תשובה ברורה\n\n"
                "שימוש: /log_mode debug | /log_mode unclear_only"
            )
            return

        normalized = mode.strip().lower()
        if normalized not in {"debug", "unclear_only"}:
            await self._send_to_admin_group(
                "רמת לוג לא חוקית.\n"
                "רמות זמינות: debug | unclear_only"
            )
            return

        await self._set_admin_log_level(normalized)
        await self._send_to_admin_group(f"עודכנה רמת לוג: {normalized}")

    async def _cmd_help(self) -> None:
        await self._send_to_admin_group(
            "\n".join(
                [
                    "פקודות ניהול זמינות:",
                    "/help - הצגת כל פקודות הניהול",
                    "/review_ok <review_id> - אישור שתשובת ג׳ימי טובה",
                    "/review_fix <review_id> <text> - יצירת טיוטת תיקון (לפני שליחה למוביל/ה)",
                    "/review_fix_page <review_id> <page refs> - יצירת תשובה מתוקנת לפי עמוד/ים מ-Notion",
                    "/review_send <review_id> - שליחת טיוטת תיקון למוביל/ה ושמירה לידע",
                    "/review_update <review_id> <text> - עדכון טיוטה קיימת או תיקון נוסף אחרי שליחה",
                    "/approve <phone> - אישור קבוצה למוביל/ה",
                    "/reset_leader <phone> - איפוס אונבורדינג למוביל/ה",
                    "/usage_report [days] - דוח שימוש (ברירת מחדל: 7 ימים)",
                    "/sync_leaders [phone] - משיכת נתוני בסיס מחדש (כולל מין) מ-Notion",
                    "/refresh_notion - רענון כפוי של תוכן Notion (FAQ/Guides)",
                    "/clear_local_cache [question] - ניקוי cache מקומי וארכוב זיכרון תשובות מתוקנות",
                    "/perf_last [count] - סיכום ביצועים אחרונים (ברירת מחדל: 20)",
                    "/set_purpose <שם עמוד> | <תיאור> - הגדרת תיאור ידני לעמוד Notion",
                    "/refresh_purposes - יצירת תיאורי עמודים מחדש באמצעות AI",
                    "/log_mode - הצגת רמת לוג נוכחית",
                    "/log_mode debug - שליחת כל הודעה ממובילים לקבוצת הניהול",
                    "/log_mode unclear_only - שליחת שאלות ללא תשובה ברורה בלבד",
                ]
            )
        )

    async def _cmd_review_fix_page(
        self, reviewer_jid: str, raw_review_id: str, page_refs: str
    ) -> None:
        review = await self._get_answer_review(raw_review_id)
        if not review:
            await self._send_to_admin_group(f"Review ID לא נמצא: {raw_review_id}")
            return
        if review.status not in {"pending", "corrected_draft", "corrected"}:
            await self._send_to_admin_group(
                f"Review {review.review_id} כבר טופל (status={review.status})."
            )
            return
        if not self._settings.notion_guides_db_id:
            await self._send_to_admin_group(
                "לא הוגדר NOTION_GUIDES_DB_ID ולכן אי אפשר לתקן לפי עמודי Notion כרגע."
            )
            return

        refs = page_refs.strip()
        if not refs:
            await self._send_to_admin_group(
                "שימוש: /review_fix_page <review_id> <page refs>\n"
                "דוגמה: /review_fix_page ABC123 3,7 או /review_fix_page ABC123 העלאת הרצאות לאתר"
            )
            return

        try:
            pages = await self._notion.get_all_guide_pages(self._settings.notion_guides_db_id)
        except Exception:
            logger.exception("Failed to load guide pages for /review_fix_page")
            await self._send_to_admin_group("לא הצלחתי לטעון את רשימת עמודי Notion כרגע.")
            return

        selected_pages = self._resolve_page_choices(refs, pages)
        if not selected_pages:
            await self._send_to_admin_group(
                "לא זיהיתי את העמודים שציינת.\n"
                "אפשר לציין מספרים (למשל 3,7) או שמות עמודים מדויקים."
            )
            return

        selected_ids = [row.get("page_id", "") for row in selected_pages if row.get("page_id")]
        selected_titles = [
            row.get("title", "ללא כותרת")
            for row in selected_pages
            if row.get("page_id")
        ]
        if not selected_ids:
            await self._send_to_admin_group("לא הצלחתי לטעון את העמודים שציינת.")
            return

        page_rows = await self._notion.get_guide_contents_strict(
            selected_ids,
            titles=selected_titles,
            source_urls=[row.get("source_url", "") for row in selected_pages],
        )
        content_by_id = {row["page_id"]: row["content"] for row in page_rows if row.get("content")}
        resolved_id_by_id = {
            row["page_id"]: row.get("resolved_page_id", row["page_id"])
            for row in page_rows
        }
        context_chunks: list[str] = []
        source_pages: list[str] = []
        source_urls: list[str] = []
        for page in selected_pages:
            page_id = page.get("page_id", "")
            title = page.get("title", "ללא כותרת")
            if not page_id:
                continue
            content = content_by_id.get(page_id, "")
            if content:
                context_chunks.append(f"Selected page title: {title}\n\n{content}")
                resolved_id = resolved_id_by_id.get(page_id, page_id)
                source_pages.append(f"{title} ({resolved_id})")
                source_urls.append(_notion_page_url(resolved_id))

        if not context_chunks:
            await self._send_to_admin_group(
                "לא מצאתי תוכן בעמודים שנבחרו. נסו עמודים אחרים."
            )
            return

        leader = await self._notion.get_leader_by_phone(
            review.leader_phone, self._settings.notion_leaders_db_id
        )
        state = await self._get_state_by_phone(review.leader_phone)
        leader_gender = (
            self._effective_leader_gender(leader, state)
            if leader and state
            else (state.preferred_gender if state and state.preferred_gender else "masculine")
        )
        result = await self._brain.respond(
            message_text=review.question_text,
            leader_name=leader.name if leader else (review.leader_name or "מוביל/ה"),
            leader_gender=leader_gender,
            leader_role=leader.role if leader and leader.role else "",
            leader_group=(
                leader.group_name if (leader and leader.has_group) else review.leader_group
            ),
            intent=Intent.ADMIN_LOGISTICS,
            clarification_allowed=False,
            forced_context="\n\n---\n\n".join(context_chunks),
        )

        await self._send_to_admin_group(
            "נוצרה תשובה מתוקנת לפי עמודי Notion שנבחרו.\n"
            f"Review: {review.review_id}\n"
            f"עמודים: {', '.join(source_pages)}\n"
            "כעת נוצרה טיוטת תיקון. נדרש /review_send כדי לשלוח למוביל/ה."
        )
        if review.status == "corrected":
            await self._cmd_review_update(
                reviewer_jid=reviewer_jid,
                raw_review_id=review.review_id,
                corrected_answer=result.response,
            )
        else:
            await self._cmd_review_fix(
                reviewer_jid=reviewer_jid,
                raw_review_id=review.review_id,
                corrected_answer=result.response,
            )

    async def _cmd_review_ok(self, reviewer_jid: str, raw_review_id: str) -> None:
        review = await self._get_answer_review(raw_review_id)
        if not review:
            await self._send_to_admin_group(f"Review ID לא נמצא: {raw_review_id}")
            return
        if review.status != "pending":
            await self._send_to_admin_group(
                f"Review {review.review_id} כבר טופל (status={review.status})."
            )
            return

        review.status = "approved"
        review.reviewer_jid = normalize_jid(reviewer_jid)
        review.reviewed_at = datetime.now(timezone.utc)
        review.updated_at = datetime.now(timezone.utc)
        self._session.add(review)
        await self._session.flush()
        await self._send_to_admin_group(f"אושר Review {review.review_id}.")

    async def _cmd_review_fix(
        self, reviewer_jid: str, raw_review_id: str, corrected_answer: str
    ) -> None:
        review = await self._get_answer_review(raw_review_id)
        if not review:
            await self._send_to_admin_group(f"Review ID לא נמצא: {raw_review_id}")
            return
        if review.status not in {"pending", "corrected_draft"}:
            await self._send_to_admin_group(
                f"Review {review.review_id} כבר טופל (status={review.status}). "
                "אפשר להשתמש ב-/review_update אם צריך תיקון נוסף."
            )
            return

        corrected = corrected_answer.strip()
        if not corrected:
            await self._send_to_admin_group("תשובה מתוקנת ריקה. שימוש: /review_fix <review_id> <text>")
            return

        review.status = "corrected_draft"
        review.corrected_answer = corrected
        review.reviewer_jid = normalize_jid(reviewer_jid)
        review.updated_at = datetime.now(timezone.utc)
        review.notion_writeback_ok = None
        review.notion_writeback_error = None
        self._session.add(review)
        await self._session.flush()
        await self._send_correction_draft_preview(review)

    async def _cmd_review_send(self, reviewer_jid: str, raw_review_id: str) -> None:
        review = await self._get_answer_review(raw_review_id)
        if not review:
            await self._send_to_admin_group(f"Review ID לא נמצא: {raw_review_id}")
            return
        if review.status != "corrected_draft":
            await self._send_to_admin_group(
                f"Review {review.review_id} לא במצב טיוטה לשליחה (status={review.status})."
            )
            return
        corrected = (review.corrected_answer or "").strip()
        if not corrected:
            await self._send_to_admin_group(
                f"ל-Review {review.review_id} אין טיוטת תיקון. השתמש/י קודם ב-/review_fix."
            )
            return

        await self._publish_corrected_answer(
            review=review,
            reviewer_jid=reviewer_jid,
            corrected_text=corrected,
            send_prefix="עדכון חשוב מהצוות:",
        )

    async def _cmd_review_update(
        self, reviewer_jid: str, raw_review_id: str, corrected_answer: str
    ) -> None:
        review = await self._get_answer_review(raw_review_id)
        if not review:
            await self._send_to_admin_group(f"Review ID לא נמצא: {raw_review_id}")
            return

        corrected = corrected_answer.strip()
        if not corrected:
            await self._send_to_admin_group(
                "תשובה מתוקנת ריקה. שימוש: /review_update <review_id> <text>"
            )
            return

        if review.status == "corrected_draft":
            review.corrected_answer = corrected
            review.reviewer_jid = normalize_jid(reviewer_jid)
            review.updated_at = datetime.now(timezone.utc)
            self._session.add(review)
            await self._session.flush()
            await self._send_correction_draft_preview(review)
            return

        if review.status == "corrected":
            await self._publish_corrected_answer(
                review=review,
                reviewer_jid=reviewer_jid,
                corrected_text=corrected,
                send_prefix="עדכון נוסף מהצוות:",
            )
            return

        await self._send_to_admin_group(
            f"Review {review.review_id} במצב {review.status} ולא ניתן לעדכן כרגע."
        )

    async def _cmd_sync_leaders(self, raw_phone: str | None) -> None:
        """Pull leader baseline data (gender/group) from Notion into local state."""
        if raw_phone:
            phone = re.sub(r"\D", "", raw_phone)
            leader = await self._notion.get_leader_by_phone(
                phone, self._settings.notion_leaders_db_id
            )
            if not leader:
                await self._send_to_admin_group(
                    f"לא נמצא מוביל/ה ב-Notion עבור הטלפון {phone}."
                )
                return

            state = await self._get_state_by_phone(phone)
            if not state:
                state = LeaderState(leader_phone=phone)
            state.preferred_gender = self._gender_from_leader(leader)
            state.group_name = leader.group_name
            state.updated_at = datetime.now(timezone.utc)
            self._session.add(state)
            await self._session.flush()
            await self._send_to_admin_group(
                "עודכנו נתוני בסיס למוביל/ה:\n"
                f"- טלפון: {phone}\n"
                f"- מין: {state.preferred_gender}\n"
                f"- קבוצה: {state.group_name or '—'}"
            )
            return

        leaders = await self._notion.get_all_leaders(self._settings.notion_leaders_db_id)
        by_phone = {leader.phone: leader for leader in leaders}

        states_result = await self._session.exec(select(LeaderState))
        states = list(states_result.all())
        updated = 0
        for state in states:
            leader = by_phone.get(state.leader_phone)
            if not leader:
                continue
            state.preferred_gender = self._gender_from_leader(leader)
            state.group_name = leader.group_name
            state.updated_at = datetime.now(timezone.utc)
            self._session.add(state)
            updated += 1

        await self._session.flush()
        await self._send_to_admin_group(
            "בוצע סנכרון נתוני בסיס מ-Notion.\n"
            f"- רשומות מקומיות שעודכנו: {updated}\n"
            f"- מובילים ב-Notion: {len(leaders)}"
        )

    async def _cmd_refresh_notion(self) -> None:
        started = time.perf_counter()
        await self._notion.clear_knowledge_cache()

        tasks: list[tuple[str, asyncio.Task]] = []
        if self._settings.notion_faq_db_id:
            tasks.append(
                (
                    "FAQ",
                    asyncio.create_task(
                        self._notion.get_faq_entries(self._settings.notion_faq_db_id)
                    ),
                )
            )
        if self._settings.notion_guides_db_id:
            tasks.append(
                (
                    "Guides",
                    asyncio.create_task(
                        self._notion.get_all_guide_documents(
                            self._settings.notion_guides_db_id
                        )
                    ),
                )
            )

        if not tasks:
            await self._send_to_admin_group(
                "בוצע ניקוי cache, אבל לא הוגדרו DB-ים של FAQ/Guides ב-.env."
            )
            return

        results = await asyncio.gather(*(task for _, task in tasks), return_exceptions=True)
        lines = ["בוצע רענון כפוי של Notion:"]
        for (name, _), result in zip(tasks, results, strict=False):
            if isinstance(result, Exception):
                lines.append(f"- {name}: נכשל ({result.__class__.__name__})")
            else:
                size = len(result) if hasattr(result, "__len__") else 0
                lines.append(f"- {name}: תקין ({size} רשומות)")

        elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
        lines.append(f"זמן כולל: {elapsed_ms}ms")
        lines.append("לרענון תיאורי עמודים: /refresh_purposes")
        await self._send_to_admin_group("\n".join(lines))

    async def _cmd_clear_local_cache(self, question_filter: str | None = None) -> None:
        """Clear runtime caches and archive local corrected-memory rows."""
        async with _clarification_lock:
            clarification_count = len(_clarification_cache)
            _clarification_cache.clear()
        async with _page_choice_lock:
            page_choice_count = len(_page_choice_cache)
            _page_choice_cache.clear()

        filter_norm = _normalize_text(question_filter or "")
        result = await self._session.exec(
            select(AnswerReview).where(AnswerReview.status == "corrected")
        )
        rows = list(result.all())
        archived = 0
        for row in rows:
            if filter_norm and _normalize_text(row.question_text) != filter_norm:
                continue
            row.status = "corrected_archived"
            row.updated_at = datetime.now(timezone.utc)
            self._session.add(row)
            archived += 1
        await self._session.flush()

        scope = (
            f"שאלה מסוננת: {question_filter.strip()}"
            if question_filter and question_filter.strip()
            else "כל הזיכרון המקומי"
        )
        await self._send_to_admin_group(
            "\n".join(
                [
                    "בוצע ניקוי cache מקומי של Jimmy:",
                    f"- היקף: {scope}",
                    f"- corrected answers בארכיון: {archived}",
                    f"- clarification cache שנוקה: {clarification_count}",
                    f"- page-choice cache שנוקה: {page_choice_count}",
                ]
            )
        )

    async def _cmd_perf_last(self, count: int) -> None:
        count = max(1, min(count, 100))
        events = get_recent_timing_events(count)
        if not events:
            await self._send_to_admin_group("אין נתוני ביצועים זמינים כרגע.")
            return

        by_stage: dict[str, list[float]] = defaultdict(list)
        for event in events:
            stage = str(event.get("stage", "unknown"))
            latency = float(event.get("latency_ms", 0.0))
            by_stage[stage].append(latency)

        lines = [f"⚡ ביצועים אחרונים (n={len(events)} אירועים):"]
        for stage, values in sorted(by_stage.items()):
            avg_ms = sum(values) / len(values)
            max_ms = max(values)
            lines.append(
                f"- {stage}: avg={avg_ms:.1f}ms, max={max_ms:.1f}ms, count={len(values)}"
            )

        await self._send_to_admin_group("\n".join(lines))

    async def _cmd_refresh_purposes(self) -> None:
        """Regenerate all auto-generated page purposes via LLM."""
        await self._send_to_admin_group("מתחיל ליצור תיאורי עמודים מחדש...")
        try:
            count = await self._brain.refresh_all_purposes()
            await self._send_to_admin_group(
                f"בוצע רענון תיאורי עמודים. עמודים שעודכנו: {count}"
            )
        except Exception:
            logger.exception("Failed to refresh purposes")
            await self._send_to_admin_group("נכשל רענון תיאורי עמודים.")

    async def _cmd_set_purpose(self, page_title: str, purpose: str) -> None:
        """Manually set the purpose of a Notion page by title."""
        if not purpose:
            await self._send_to_admin_group(
                "שימוש: /set_purpose <שם עמוד> | <תיאור>"
            )
            return

        if not self._settings.notion_guides_db_id:
            await self._send_to_admin_group("לא הוגדר NOTION_GUIDES_DB_ID.")
            return

        try:
            pages = await self._notion.get_all_guide_pages(
                self._settings.notion_guides_db_id
            )
        except Exception:
            logger.exception("Failed to load guide pages for /set_purpose")
            await self._send_to_admin_group("לא הצלחתי לטעון את רשימת העמודים.")
            return

        target_norm = _normalize_text(page_title)
        matched_page: dict[str, str] | None = None
        for page in pages:
            if _normalize_text(page.get("title", "")) == target_norm:
                matched_page = page
                break

        if not matched_page:
            for page in pages:
                if target_norm in _normalize_text(page.get("title", "")):
                    matched_page = page
                    break

        if not matched_page:
            await self._send_to_admin_group(
                f"לא נמצא עמוד בשם \"{page_title}\".\n"
                "אפשר לציין שם מדויק מרשימת העמודים ב-Notion."
            )
            return

        from models.notion_page_meta import NotionPageMeta

        page_id = matched_page["page_id"]
        now = datetime.now(timezone.utc)
        stmt = select(NotionPageMeta).where(
            NotionPageMeta.notion_page_id == page_id
        )
        result = await self._session.exec(stmt)
        existing = result.first()
        if existing:
            existing.purpose = purpose
            existing.title = matched_page.get("title", existing.title)
            existing.is_auto_generated = False
            existing.updated_at = now
            self._session.add(existing)
        else:
            row = NotionPageMeta(
                notion_page_id=page_id,
                title=matched_page.get("title", ""),
                purpose=purpose,
                source_type="guide",
                is_auto_generated=False,
                created_at=now,
                updated_at=now,
            )
            self._session.add(row)
        await self._session.flush()

        await self._send_to_admin_group(
            f"עודכן תיאור עמוד \"{matched_page.get('title', '')}\":\n{purpose}"
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _send_dm(self, jid: str, text: str) -> None:
        jid = normalize_jid(jid)
        await self._wa.send_message(
            SendMessageRequest(phone=jid, message=_format_whatsapp_markup(text))
        )

    async def _send_to_admin_group(self, text: str) -> None:
        group_id = self._settings.admin_whatsapp_group_id
        if not group_id:
            logger.warning("ADMIN_WHATSAPP_GROUP_ID not configured; skipping admin msg")
            return
        await self._wa.send_message(
            SendMessageRequest(phone=group_id, message=_format_whatsapp_markup(text))
        )

    async def _store_escalation(
        self, leader: LeaderRecord, question: str, intent_type: str
    ) -> None:
        eq = EscalatedQuestion(
            leader_phone=leader.phone,
            leader_name=leader.name,
            leader_group=leader.group_name,
            question_text=question,
            intent_type=intent_type,
        )
        self._session.add(eq)
        await self._session.flush()

    async def _notify_uncertain_source_selection(
        self,
        leader: LeaderRecord,
        question: str,
        intent: str,
        uncertainty_reason: str | None,
    ) -> None:
        reason = uncertainty_reason or "needs_source_selection"
        await self._send_to_admin_group(
            "\n".join(
                [
                    "⚠️ נדרש בחירת מקור לפני תשובה",
                    f"כוונה: {intent}",
                    f"סיבה: {reason}",
                    f"מוביל/ה: {leader.name}",
                    f"קבוצה: {leader.group_name or '—'}",
                    f"טלפון: {leader.phone}",
                    "שאלה:",
                    question,
                    "",
                    "ג׳ימי ביקש מהמנהל/ת לבחור עמוד/ים רלוונטיים מ-Notion לפני מענה.",
                ]
            )
        )
        await self._store_escalation(
            leader=leader,
            question=question,
            intent_type=f"{intent}_SOURCE_SELECTION_REQUIRED",
        )

    async def _create_answer_review(
        self,
        leader: LeaderRecord,
        question: str,
        bot_answer: str,
        intent_type: str,
        leader_jid: str,
    ) -> str:
        review_id = self._new_review_id()
        row = AnswerReview(
            review_id=review_id,
            leader_phone=leader.phone,
            leader_jid=leader_jid,
            leader_name=leader.name,
            leader_group=leader.group_name,
            question_text=question,
            bot_answer=bot_answer,
            intent_type=intent_type,
            status="pending",
            updated_at=datetime.now(timezone.utc),
        )
        self._session.add(row)
        await self._session.flush()
        return review_id

    async def _get_answer_review(self, review_id: str) -> AnswerReview | None:
        normalized = review_id.strip().upper()
        stmt = select(AnswerReview).where(AnswerReview.review_id == normalized)
        result = await self._session.exec(stmt)
        return result.first()

    async def _get_local_corrections(
        self, question_text: str, limit: int = 50
    ) -> list[dict[str, str]]:
        normalized = _normalize_text(question_text)
        if not normalized:
            return []
        stmt = (
            select(AnswerReview)
            .where(AnswerReview.status == "corrected")
            .order_by(AnswerReview.reviewed_at.desc())
            .limit(limit)
        )
        result = await self._session.exec(stmt)
        rows = result.all()
        return [
            {
                "question_text": row.question_text,
                "corrected_answer": row.corrected_answer or "",
            }
            for row in rows
            if row.corrected_answer
        ]

    def _new_review_id(self) -> str:
        return uuid.uuid4().hex[:10].upper()

    async def _send_debug_trace(
        self,
        leader: LeaderRecord,
        question: str,
        answer: str,
        intent: str,
        needs_clarification: bool,
        should_escalate: bool,
        review_id: str | None = None,
        is_grounded: bool | None = None,
        source_count: int | None = None,
        source_pages: list[str] | None = None,
        flow_mode: str | None = None,
        uncertainty_reason: str | None = None,
    ) -> None:
        source_pages = source_pages or []
        await self._send_to_admin_group(
            "\n".join(
                [
                    "🐞 DEBUG | שאלה ותשובה",
                    f"מוביל/ה: {leader.name}",
                    f"קבוצה: {leader.group_name or '—'}",
                    f"טלפון: {leader.phone}",
                    f"אינטנט: {intent}",
                    f"Clarification: {needs_clarification}",
                    f"Escalate: {should_escalate}",
                    f"Review ID: {review_id or '—'}",
                    f"Grounded: {is_grounded if is_grounded is not None else 'n/a'}",
                    f"Sources: {source_count if source_count is not None else 'n/a'}",
                    f"Flow: {flow_mode or 'n/a'}",
                    f"Uncertainty: {uncertainty_reason or 'n/a'}",
                    (
                        "Source Pages: " + " | ".join(source_pages)
                        if source_pages
                        else "Source Pages: n/a"
                    ),
                    "שאלה:",
                    f"\"{question}\"",
                    "",
                    "תשובת ג׳ימי:",
                    answer,
                    "",
                    "לאישור: /review_ok <review_id>",
                    "לתיקון טיוטה: /review_fix <review_id> <תשובה מתוקנת>",
                    "לתיקון לפי עמוד/ים: /review_fix_page <review_id> <page refs>",
                    "לשליחת טיוטה: /review_send <review_id>",
                ]
            )
        )

    async def _send_correction_draft_preview(self, review: AnswerReview) -> None:
        corrected = (review.corrected_answer or "").strip()
        await self._send_to_admin_group(
            "\n".join(
                [
                    f"טיוטת תיקון מוכנה (Review {review.review_id}):",
                    "",
                    corrected or "—",
                    "",
                    "אם נדרש שינוי נוסף: /review_update <review_id> <text>",
                    "לאישור שליחה למוביל/ה: /review_send <review_id>",
                ]
            )
        )

    async def _publish_corrected_answer(
        self,
        review: AnswerReview,
        reviewer_jid: str,
        corrected_text: str,
        send_prefix: str,
    ) -> None:
        review.corrected_answer = corrected_text
        review.reviewer_jid = normalize_jid(reviewer_jid)
        review.reviewed_at = datetime.now(timezone.utc)
        review.updated_at = datetime.now(timezone.utc)
        review.notion_writeback_ok = None
        review.notion_writeback_error = None
        self._session.add(review)
        await self._session.flush()

        if self._settings.notion_faq_db_id:
            try:
                notion_page_id = await self._notion.add_faq_correction(
                    db_id=self._settings.notion_faq_db_id,
                    question_text=review.question_text,
                    answer_text=corrected_text,
                )
                review.notion_writeback_ok = True
                review.notion_writeback_error = None
                review.updated_at = datetime.now(timezone.utc)
                self._session.add(review)
                await self._session.flush()
                await self._send_to_admin_group(
                    f"נשמרה תשובה מתוקנת גם ב-Notion (page_id: {notion_page_id})."
                )
            except Exception as exc:
                logger.exception("Failed to write corrected answer to Notion FAQ")
                review.notion_writeback_ok = False
                review.notion_writeback_error = str(exc)
                review.updated_at = datetime.now(timezone.utc)
                self._session.add(review)
                await self._session.flush()
                await self._send_to_admin_group(
                    "התשובה תוקנה מקומית אבל כתיבה ל-Notion נכשלה.\n"
                    f"Review: {review.review_id}\n"
                    f"Error: {exc.__class__.__name__}"
                )

        try:
            await self._send_dm(
                review.leader_jid,
                "\n".join([send_prefix, corrected_text]),
            )
        except Exception:
            logger.exception("Failed sending corrected answer to leader")
            await self._send_to_admin_group(
                "התשובה תוקנה אבל לא הצלחתי לשלוח עדכון למוביל/ה.\n"
                f"Review: {review.review_id}\n"
                f"Leader JID: {review.leader_jid}"
            )
            return

        review.status = "corrected"
        review.updated_at = datetime.now(timezone.utc)
        self._session.add(review)
        await self._session.flush()
        await self._send_to_admin_group(
            f"התשובה תוקנה ונשלחה למוביל/ה. Review {review.review_id}"
        )

    async def _offer_page_choice(
        self,
        sender_jid: str,
        sender_key: str,
        question: str,
        leader: LeaderRecord,
    ) -> bool:
        """Offer all guide pages for user-assisted disambiguation."""
        if not self._settings.notion_guides_db_id:
            return False
        try:
            pages = await self._notion.get_all_guide_pages(self._settings.notion_guides_db_id)
        except Exception:
            logger.exception("Failed to fetch guide pages for page-choice fallback")
            return False

        if not pages:
            return False

        async with _page_choice_lock:
            _page_choice_cache[sender_key] = {
                "question": question,
                "pages": pages,
                "leader_name": leader.name,
            }

        intro = (
            "לפני שאני עונה, בחר/י עמוד אחד או יותר מהאינדקס של Notion שנראים הכי רלוונטיים לשאלה.\n"
            "אפשר לענות עם מספרים (למשל: 2,5,9) או עם שמות עמודים."
        )
        await self._send_dm(sender_jid, intro)

        chunk_size = 20
        for start in range(0, len(pages), chunk_size):
            batch = pages[start : start + chunk_size]
            lines = [
                f"{start + idx + 1}. {page.get('title', 'ללא כותרת')}"
                for idx, page in enumerate(batch)
            ]
            await self._send_dm(sender_jid, "\n".join(lines))

        await self._send_dm(
            sender_jid,
            "אחרי הבחירה אענה לפי התוכן של כל העמודים שבחרת.",
        )
        return True

    async def _handle_pending_page_choice(
        self,
        sender_jid: str,
        sender_key: str,
        message_text: str,
        leader: LeaderRecord,
        state: LeaderState,
    ) -> bool:
        async with _page_choice_lock:
            pending = _page_choice_cache.get(sender_key)
        if not pending:
            return False

        text = message_text.strip()
        if text.lower() in {"דלג", "skip", "בטל", "ביטול", "cancel"}:
            async with _page_choice_lock:
                _page_choice_cache.pop(sender_key, None)
            await self._send_dm(sender_jid, "בוטל. אפשר לשאול מחדש ואנסה שוב.")
            return True

        pages = pending.get("pages", [])
        selected_pages = self._resolve_page_choices(text, pages)
        if not selected_pages:
            await self._send_dm(
                sender_jid,
                "לא הצלחתי לזהות עמודים מהרשימה. כתוב/כתבי מספר אחד או יותר (למשל 1,4) או שמות עמודים מהרשימה.",
            )
            return True

        selected_ids = [row.get("page_id", "") for row in selected_pages if row.get("page_id")]
        selected_titles = [
            row.get("title", "ללא כותרת")
            for row in selected_pages
            if row.get("page_id")
        ]
        if not selected_ids:
            await self._send_dm(
                sender_jid, "לא הצלחתי לטעון את העמודים שנבחרו. נסו לבחור שוב."
            )
            return True

        page_rows = await self._notion.get_guide_contents_strict(
            selected_ids,
            titles=selected_titles,
            source_urls=[row.get("source_url", "") for row in selected_pages],
        )
        content_by_id = {row["page_id"]: row["content"] for row in page_rows if row.get("content")}
        resolved_id_by_id = {
            row["page_id"]: row.get("resolved_page_id", row["page_id"])
            for row in page_rows
        }
        selected_context_chunks: list[str] = []
        source_pages: list[str] = []
        source_urls: list[str] = []
        sparse_source_urls: list[str] = []
        for page in selected_pages:
            page_id = page.get("page_id", "")
            title = page.get("title", "ללא כותרת")
            if not page_id:
                continue
            content = content_by_id.get(page_id, "")
            if content:
                selected_context_chunks.append(
                    f"Selected page title: {title}\n\n{content}"
                )
                resolved_id = resolved_id_by_id.get(page_id, page_id)
                source_pages.append(f"{title} ({resolved_id})")
                direct_url = (page.get("source_url", "") or "").strip() or _notion_page_url(
                    resolved_id
                )
                source_urls.append(direct_url)
                if len(content.strip()) < 280:
                    sparse_source_urls.append(direct_url)

        if not selected_context_chunks:
            await self._send_dm(
                sender_jid,
                "לא מצאתי תוכן בעמודים שנבחרו. אפשר לבחור עמודים אחרים מהרשימה.",
            )
            return True

        original_question = str(pending.get("question", "")).strip() or message_text
        await self._send_dm(
            sender_jid,
            "אני עונה לפי העמודים שנבחרו:\n" + "\n".join(f"- {page}" for page in source_pages),
        )
        leader_gender = self._effective_leader_gender(leader, state)
        result = await self._brain.respond(
            message_text=original_question,
            leader_name=leader.name,
            leader_gender=leader_gender,
            leader_role=leader.role or "",
            leader_group=leader.group_name if leader.has_group else None,
            intent=Intent.ADMIN_LOGISTICS,
            clarification_allowed=False,
            forced_context="\n\n---\n\n".join(selected_context_chunks),
        )
        answer_with_sources = result.response
        if sparse_source_urls:
            answer_with_sources += (
                "\n\nהעמוד שנבחר כולל מעט טקסט (לרוב בגלל צילומי מסך/תמונות), "
                "ולכן מומלץ לקרוא את ההנחיות המלאות ישירות ב-Notion."
            )
        if source_urls:
            answer_with_sources += "\n\nלפירוט נוסף:\n" + "\n".join(
                f"- {url}" for url in _dedupe_list(source_urls)
            )
        await self._send_dm(sender_jid, answer_with_sources)

        admin_log_level = await self._get_admin_log_level()
        if admin_log_level == "debug":
            await self._send_debug_trace(
                leader=leader,
                question=original_question,
                answer=result.response,
                intent="ADMIN_LOGISTICS_PAGE_CHOICE",
                needs_clarification=False,
                should_escalate=result.should_escalate,
                is_grounded=result.is_grounded,
                source_count=result.source_count,
                source_pages=source_pages,
                flow_mode="menu_selected_pages",
                uncertainty_reason=result.uncertainty_reason,
            )

        if result.should_escalate:
            await self._send_to_admin_group(
                T.ADMIN_ESCALATION_LOGISTICS.format(
                    movil="מוביל" if leader_gender == "masculine" else "מובילה",
                    leader_name=leader.name,
                    leader_group=leader.group_name or "—",
                    leader_phone=leader.phone,
                    original_question=original_question,
                    intent_type="ADMIN_LOGISTICS_PAGE_CHOICE",
                )
            )
            await self._store_escalation(
                leader,
                original_question,
                "ADMIN_LOGISTICS_PAGE_CHOICE",
            )

        await self._clear_clarification_state(sender_key)
        async with _page_choice_lock:
            _page_choice_cache.pop(sender_key, None)
        return True

    def _resolve_page_choices(
        self, message_text: str, pages: list[dict[str, str]]
    ) -> list[dict[str, str]]:
        text = message_text.strip()
        selected: list[dict[str, str]] = []
        selected_ids: set[str] = set()

        # Strict numeric mode: if user provided numbers, resolve only exact indexes.
        number_tokens = re.findall(r"\d+", text)
        if number_tokens:
            for match in number_tokens:
                idx = int(match) - 1
                if 0 <= idx < len(pages):
                    row = pages[idx]
                    page_id = row.get("page_id", "")
                    if page_id and page_id not in selected_ids:
                        selected.append(row)
                        selected_ids.add(page_id)
            return selected

        tokens = [
            _normalize_text(tok)
            for tok in re.split(r"[,\n;]+", text)
            if _normalize_text(tok)
        ]
        for token in tokens:
            for page in pages:
                page_id = page.get("page_id", "")
                if not page_id or page_id in selected_ids:
                    continue
                title = _normalize_text(page.get("title", ""))
                if title and (token == title or token in title or title in token):
                    selected.append(page)
                    selected_ids.add(page_id)
                    break

        return selected

    async def _handle_explicit_page_answer_request(
        self,
        sender_jid: str,
        message_text: str,
        leader: LeaderRecord,
        state: LeaderState,
    ) -> bool:
        """Allow leader to force-answer from specific Notion guide pages.

        Supported formats:
        - /answer_from_page <page refs> | <question>
        - /answer_from_pages <page refs> | <question>
        """
        match = re.match(
            r"^/(?:answer_from_page|answer_from_pages)\s+(.+?)\s*\|\s*(.+)$",
            message_text.strip(),
            flags=re.DOTALL | re.IGNORECASE,
        )
        if not match:
            return False

        if not self._settings.notion_guides_db_id:
            await self._send_dm(
                sender_jid,
                "לא הוגדר NOTION_GUIDES_DB_ID ולכן אי אפשר לענות לפי עמודים כרגע.",
            )
            return True

        page_refs = match.group(1).strip()
        question = match.group(2).strip()
        if not page_refs or not question:
            await self._send_dm(
                sender_jid,
                "שימוש נכון:\n/answer_from_page <מספר/שם עמוד או כמה עמודים מופרדים בפסיק> | <השאלה>",
            )
            return True

        try:
            pages = await self._notion.get_all_guide_pages(self._settings.notion_guides_db_id)
        except Exception:
            logger.exception("Failed to load guide pages for explicit page answer request")
            await self._send_dm(sender_jid, "לא הצלחתי לטעון את רשימת העמודים כרגע. נסה/י שוב בעוד רגע.")
            return True

        selected_pages = self._resolve_page_choices(page_refs, pages)
        if not selected_pages:
            await self._send_dm(
                sender_jid,
                "לא זיהיתי את העמודים שציינת.\n"
                "אפשר לציין מספרים (למשל 3,7) או שמות עמודים מדויקים.\n"
                "פורמט: /answer_from_page <page refs> | <question>",
            )
            return True

        selected_ids = [row.get("page_id", "") for row in selected_pages if row.get("page_id")]
        selected_titles = [
            row.get("title", "ללא כותרת")
            for row in selected_pages
            if row.get("page_id")
        ]
        if not selected_ids:
            await self._send_dm(sender_jid, "לא הצלחתי לטעון את העמודים שציינת.")
            return True

        page_rows = await self._notion.get_guide_contents_strict(
            selected_ids,
            titles=selected_titles,
            source_urls=[row.get("source_url", "") for row in selected_pages],
        )
        content_by_id = {row["page_id"]: row["content"] for row in page_rows if row.get("content")}
        resolved_id_by_id = {
            row["page_id"]: row.get("resolved_page_id", row["page_id"])
            for row in page_rows
        }
        context_chunks: list[str] = []
        source_pages: list[str] = []
        source_urls: list[str] = []
        sparse_source_urls: list[str] = []
        for page in selected_pages:
            page_id = page.get("page_id", "")
            title = page.get("title", "ללא כותרת")
            if not page_id:
                continue
            content = content_by_id.get(page_id, "")
            if content:
                context_chunks.append(f"Selected page title: {title}\n\n{content}")
                resolved_id = resolved_id_by_id.get(page_id, page_id)
                source_pages.append(f"{title} ({resolved_id})")
                direct_url = (page.get("source_url", "") or "").strip() or _notion_page_url(
                    resolved_id
                )
                source_urls.append(direct_url)
                if len(content.strip()) < 280:
                    sparse_source_urls.append(direct_url)

        if not context_chunks:
            await self._send_dm(
                sender_jid,
                "לא מצאתי תוכן בעמודים שבחרת. נסה/י לבחור עמודים אחרים.",
            )
            return True

        leader_gender = self._effective_leader_gender(leader, state)
        await self._send_dm(
            sender_jid,
            "אני עונה לפי העמודים שבחרת:\n" + "\n".join(f"- {page}" for page in source_pages),
        )
        result = await self._brain.respond(
            message_text=question,
            leader_name=leader.name,
            leader_gender=leader_gender,
            leader_role=leader.role or "",
            leader_group=leader.group_name if leader.has_group else None,
            intent=Intent.ADMIN_LOGISTICS,
            clarification_allowed=False,
            forced_context="\n\n---\n\n".join(context_chunks),
        )
        answer_with_sources = result.response
        if sparse_source_urls:
            answer_with_sources += (
                "\n\nהעמוד/ים שנבחרו כוללים מעט טקסט (כנראה צילומי מסך/תמונות), "
                "ולכן מומלץ לקרוא את ההנחיות המלאות ישירות ב-Notion."
            )
        if source_urls:
            answer_with_sources += "\n\nלפירוט נוסף:\n" + "\n".join(
                f"- {url}" for url in _dedupe_list(source_urls)
            )
        await self._send_dm(sender_jid, answer_with_sources)

        admin_log_level = await self._get_admin_log_level()
        if admin_log_level == "debug":
            review_id = await self._create_answer_review(
                leader=leader,
                question=question,
                bot_answer=result.response,
                intent_type="ADMIN_LOGISTICS_FORCED_PAGE",
                leader_jid=normalize_jid(sender_jid),
            )
            await self._send_debug_trace(
                leader=leader,
                question=f"[forced pages: {', '.join(selected_titles)}]\n{question}",
                answer=result.response,
                intent="ADMIN_LOGISTICS_FORCED_PAGE",
                needs_clarification=False,
                should_escalate=result.should_escalate,
                review_id=review_id,
                is_grounded=result.is_grounded,
                source_count=result.source_count,
                source_pages=source_pages,
                flow_mode="forced_page_command",
                uncertainty_reason=result.uncertainty_reason,
            )

        return True

    async def _get_or_create_state(self, phone: str) -> LeaderState:
        stmt = select(LeaderState).where(LeaderState.leader_phone == phone)
        result = await self._session.exec(stmt)
        state = result.first()
        if state:
            return state
        state = LeaderState(leader_phone=phone)
        self._session.add(state)
        await self._session.flush()
        return state

    async def _get_state_by_phone(self, phone: str) -> LeaderState | None:
        stmt = select(LeaderState).where(LeaderState.leader_phone == phone)
        result = await self._session.exec(stmt)
        return result.first()

    async def _clarification_allowed(self, sender_key: str) -> bool:
        async with _clarification_lock:
            return sender_key not in _clarification_cache

    async def _mark_clarification_asked(self, sender_key: str) -> None:
        async with _clarification_lock:
            _clarification_cache[sender_key] = True

    async def _clear_clarification_state(self, sender_key: str) -> None:
        async with _clarification_lock:
            _clarification_cache.pop(sender_key, None)

    async def _get_admin_log_level(self) -> str:
        async with _admin_log_level_lock:
            return _admin_log_level

    async def _set_admin_log_level(self, level: str) -> None:
        global _admin_log_level
        async with _admin_log_level_lock:
            _admin_log_level = level

    def _effective_leader_gender(self, leader: LeaderRecord, state: LeaderState) -> str:
        if state.preferred_gender in {"masculine", "feminine"}:
            return state.preferred_gender
        return self._gender_from_leader(leader)

    def _gender_from_leader(self, leader: LeaderRecord) -> str:
        return leader.gender_kind or "masculine"


def _normalize_text(text: str) -> str:
    cleaned = re.sub(r"[^\w\s\u0590-\u05FF]", " ", text.lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _format_whatsapp_markup(text: str) -> str:
    """Normalize bold markers for WhatsApp formatting.

    WhatsApp bold uses *text* (single asterisk), while many LLM outputs use
    markdown-style **text**.
    """
    if not text:
        return text
    return re.sub(r"\*\*(.+?)\*\*", r"*\1*", text, flags=re.DOTALL)


def _notion_page_url(page_id: str) -> str:
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"


def _dedupe_list(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out
