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
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from cachetools import TTLCache
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from config import Settings
from jimmy.brain import Intent, JimmyBrain
from jimmy.notion_client import LeaderRecord, NotionClient
from jimmy import templates as T
from models.escalated_question import EscalatedQuestion
from models.leader_state import LeaderState, OnboardingStage
from models.message import Message
from whatsapp import SendMessageRequest, WhatsAppClient
from whatsapp.jid import normalize_jid

logger = logging.getLogger(__name__)
_clarification_cache = TTLCache(maxsize=2000, ttl=10 * 60)
_clarification_lock = asyncio.Lock()


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

        await self._converse(sender_jid, message_text, leader)

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
        state.updated_at = datetime.now(timezone.utc)
        self._session.add(state)
        await self._session.flush()

    # ------------------------------------------------------------------
    # Conversation (all post-onboarding messages go through the LLM)
    # ------------------------------------------------------------------

    async def _converse(
        self, sender_jid: str, message_text: str, leader: LeaderRecord
    ) -> None:
        classification = await self._brain.classify_intent(message_text)
        sender_key = normalize_jid(sender_jid)
        clarification_allowed = await self._clarification_allowed(sender_key)

        result = await self._brain.respond(
            message_text=message_text,
            leader_name=leader.name,
            leader_gender="masculine" if leader.is_masculine else "feminine",
            leader_role=leader.role or "",
            leader_group=leader.group_name if leader.has_group else None,
            intent=classification.intent,
            event_type=classification.event_type,
            clarification_allowed=clarification_allowed,
        )

        if result.needs_clarification:
            await self._send_dm(
                sender_jid,
                result.clarification_question or result.response,
            )
            g = T._g(leader)
            await self._send_to_admin_group(
                T.ADMIN_ESCALATION_LOGISTICS.format(
                    movil=g["movil"],
                    leader_name=leader.name,
                    leader_group=leader.group_name or "—",
                    leader_phone=leader.phone,
                    original_question=message_text,
                    intent_type=f"{result.intent.value}_CLARIFICATION",
                )
            )
            await self._store_escalation(
                leader, message_text, f"{result.intent.value}_CLARIFICATION"
            )
            await self._mark_clarification_asked(sender_key)
            return

        await self._clear_clarification_state(sender_key)
        await self._send_dm(sender_jid, result.response)

        if result.should_escalate:
            g = T._g(leader)
            await self._send_to_admin_group(
                T.ADMIN_ESCALATION_LOGISTICS.format(
                    movil=g["movil"],
                    leader_name=leader.name,
                    leader_group=leader.group_name or "—",
                    leader_phone=leader.phone,
                    original_question=message_text,
                    intent_type=result.intent.value,
                )
            )
            await self._store_escalation(
                leader, message_text, result.intent.value
            )

    # ------------------------------------------------------------------
    # Admin commands
    # ------------------------------------------------------------------

    async def _handle_admin_command(self, sender_jid: str, message_text: str) -> None:
        text = message_text.strip()

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

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _send_dm(self, jid: str, text: str) -> None:
        jid = normalize_jid(jid)
        await self._wa.send_message(SendMessageRequest(phone=jid, message=text))

    async def _send_to_admin_group(self, text: str) -> None:
        group_id = self._settings.admin_whatsapp_group_id
        if not group_id:
            logger.warning("ADMIN_WHATSAPP_GROUP_ID not configured; skipping admin msg")
            return
        await self._wa.send_message(SendMessageRequest(phone=group_id, message=text))

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
