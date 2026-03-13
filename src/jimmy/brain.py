"""JimmyBrain – conversational LLM-based assistant.

Every message goes through the LLM. The brain:
1. Classifies intent in the background (for escalation/routing decisions)
2. Fetches Notion context when needed
3. Generates a natural, conversational Hebrew response
"""

import asyncio
import enum
import logging
import re
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

from pydantic_ai import Agent
from sqlalchemy import text as sa_text

from config import Settings
from jimmy.embeddings import embed_query
from jimmy.notion_client import NotionClient

logger = logging.getLogger(__name__)
_timing_events: deque[dict] = deque(maxlen=500)


class Intent(str, enum.Enum):
    ADMIN_LOGISTICS = "ADMIN_LOGISTICS"
    TEMPLATES = "TEMPLATES"
    META_PROGRAM = "META_PROGRAM"
    FACILITATION = "FACILITATION"
    GREETING = "GREETING"
    UNKNOWN = "UNKNOWN"


CLASSIFICATION_SYSTEM_PROMPT = """\
You are an intent classifier for a Hebrew-speaking administrative assistant bot \
called Jimmy, used by TechGym group leaders.

Classify the following user message into exactly ONE of these intents:

- GREETING: greetings, casual hello, thanks, small talk, "היי", "תודה", "מה קורה".
- ADMIN_LOGISTICS: questions about schedules, logistics, dates, what to prepare, \
weekly tasks, administrative procedures.
- TEMPLATES: requests for message templates to send in their participant group \
(e.g. "send me the reminder text", "what do I write to the group?").
- META_PROGRAM: questions about the program structure, steps, stages, milestones, \
what happens when (e.g. "what are the stages?", "when is the final meetup?").
- FACILITATION: questions about participant behavior, group dynamics, motivation, \
how to engage participants, camera issues, interpersonal conflict, emotional states, \
psychological advice, "how do I deal with a quiet participant?", \
"what if someone is dominant?". This includes ANY question about how to handle people.
- UNKNOWN: cannot determine the intent.

Respond with ONLY the intent name, nothing else. No explanation.
"""

CONVERSATION_SYSTEM_PROMPT = """\
You are Jimmy, a male Hebrew-speaking administrative assistant bot for TechGym group leaders.
You always refer to yourself in masculine Hebrew form (אני יכול, אני בטוח, etc.).

About you:
- You help with administrative questions: schedules, logistics, program stages, \
templates, reminders, dates.
- You are friendly, concise, and helpful. Use a warm but professional tone.
- You NEVER give facilitation, psychological, interpersonal, or emotional advice. \
If asked about those topics, warmly decline and suggest asking in the leaders group.
- You NEVER invent program rules or information not in the provided context.
- Keep answers short and natural (2-5 sentences for questions, 1-2 for casual chat).
- Always respond in Hebrew.
- Greeting policy:
  - If intent is GREETING: a short friendly greeting is good.
  - If intent is NOT GREETING: do NOT open with "היי", "שלום", or similar greeting phrases.

Important context: when a leader uses "אני" (I/me) in their question, they are \
always referring to themselves as a group leader in the TechGym program. \
For example "האם אני חייבת להעביר הרצאה?" means "Am I required, as a leader, to \
facilitate a lecture in my group?". Always interpret questions in the context of \
the leader's role in the program.

About the leader you're talking to:
- Name: {leader_name}
- Gender: {leader_gender}
- Role: {leader_role}
- Intent: {intent_name}
{group_info}

{context_section}\
Address the leader appropriately based on their gender \
(masculine: אתה, שלך / feminine: את, שלך).
"""

EVENT_TYPE_SYSTEM_PROMPT = """\
You are a parser that extracts the event type from a Hebrew message requesting a \
message template. The possible event types are:

- weekly_meeting
- kickoff
- dry_run
- final_meetup
- mid_program_checkin
- general

Respond with ONLY the event type, nothing else. If unclear, respond with "general".
"""

PAGE_SELECTION_SYSTEM_PROMPT = """\
You are a search relevance assistant. Given a user question and a list of \
knowledge-base page titles, return ONLY the numbers of the pages most likely \
to contain the answer (up to 5), comma-separated. If none seem relevant, return 'none'.
"""

CLARIFICATION_SYSTEM_PROMPT = """\
You are Jimmy, a male Hebrew-speaking administrative assistant for TechGym.
Write exactly ONE short clarifying question in Hebrew to help answer the user's
administrative question better.

Rules:
- Ask one concrete question only.
- Do not answer the original question yet.
- Keep it short (one sentence).
- No bullet points.
- If relevant, ask about event/stage/date/cohort or which specific process they mean.
"""

GROUNDING_VALIDATION_SYSTEM_PROMPT = """\
You are a grounding validator.
Decide if the assistant answer is reasonably supported by the provided knowledge context.

Rules:
- Return ONLY one token: grounded OR not_grounded.
- Return grounded if the answer's key claims can be inferred from or are consistent \
with the context, even if the answer paraphrases or summarizes.
- Return not_grounded only if the answer makes specific factual claims that clearly \
contradict the context, or invents concrete details (dates, rules, URLs) not in context.
- Return not_grounded if the answer recommends a different platform/process than context.
- If the context covers the topic and the answer is a reasonable interpretation, \
return grounded.
"""

PURPOSE_GENERATION_SYSTEM_PROMPT = """\
You generate concise Hebrew descriptions of knowledge-base pages for a program \
called TechGym. Each description should capture what the page is about and what \
questions it can answer, in 1-2 sentences.

Rules:
- Write in Hebrew.
- Be specific: mention key topics, event types, dates, processes.
- Keep it under 200 characters.
- Do NOT include the page title in the description.
"""


@dataclass
class ClassificationResult:
    intent: Intent
    event_type: str | None = None


@dataclass
class ConversationResult:
    response: str
    intent: Intent
    is_confident: bool
    should_escalate: bool
    needs_clarification: bool = False
    clarification_question: str | None = None
    is_grounded: bool | None = None
    source_count: int = 0
    uncertainty_reason: str | None = None
    source_titles: list[str] | None = None
    source_page_ids: list[str] | None = None


@dataclass
class KnowledgeContextResult:
    context_text: str
    source_titles: list[str]
    has_source_conflict: bool = False
    from_rag: bool = False
    source_page_ids: list[str] | None = None


class JimmyBrain:
    def __init__(
        self,
        settings: Settings,
        notion: NotionClient,
        session_factory: Any | None = None,
    ):
        self._settings = settings
        self._notion = notion
        self._session_factory = session_factory

    async def retrieve_chunks(
        self, question: str, top_k: int = 8
    ) -> list[dict[str, str]]:
        """Retrieve the most relevant knowledge chunks via pgvector cosine similarity."""
        if not self._session_factory or not self._settings.voyage_api_key:
            return []
        try:
            started = time.perf_counter()
            query_embedding = await embed_query(
                question, self._settings.voyage_api_key
            )
            async with self._session_factory() as session:
                result = await session.execute(
                    sa_text(
                        "SELECT chunk_text, page_title, notion_page_id "
                        "FROM knowledge_chunk "
                        "ORDER BY embedding <=> :emb "
                        "LIMIT :k"
                    ),
                    {"emb": str(query_embedding), "k": top_k},
                )
                rows = result.fetchall()
            chunks = [
                {
                    "chunk_text": row[0],
                    "page_title": row[1],
                    "notion_page_id": row[2],
                }
                for row in rows
            ]
            self._log_latency(
                "rag_retrieval",
                started,
                {"chunks_found": len(chunks), "top_k": top_k},
            )
            return chunks
        except Exception:
            logger.exception("RAG retrieval failed, falling back to legacy flow")
            return []

    async def classify_intent(self, message_text: str) -> ClassificationResult:
        started = time.perf_counter()
        heuristic = _heuristic_intent(message_text)
        if heuristic:
            self._log_latency(
                "intent_classification",
                started,
                {
                    "intent": heuristic.intent.value,
                    "has_event_type": bool(heuristic.event_type),
                    "source": "heuristic",
                },
            )
            return heuristic

        agent = Agent(
            model=self._settings.model_name,
            system_prompt=CLASSIFICATION_SYSTEM_PROMPT,
            output_type=str,
        )
        result = await agent.run(message_text)
        raw = result.output.strip().upper()

        try:
            intent = Intent(raw)
        except ValueError:
            logger.warning("LLM returned unknown intent: %s", raw)
            intent = Intent.UNKNOWN

        event_type = None
        if intent == Intent.TEMPLATES:
            event_type = await self._extract_event_type(message_text)

        self._log_latency(
            "intent_classification",
            started,
            {
                "intent": intent.value,
                "has_event_type": bool(event_type),
                "source": "llm",
            },
        )
        return ClassificationResult(intent=intent, event_type=event_type)

    async def respond(
        self,
        message_text: str,
        leader_name: str,
        leader_gender: str,
        leader_role: str,
        leader_group: str | None,
        intent: Intent,
        event_type: str | None = None,
        clarification_allowed: bool = True,
        forced_context: str | None = None,
        local_corrections: list[dict[str, str]] | None = None,
        rag_chunks: list[dict[str, str]] | None = None,
    ) -> ConversationResult:
        """Generate a natural conversational response, fetching Notion context when needed."""
        response_started = time.perf_counter()
        context_section = ""
        context_sources: list[str] = []
        context_page_ids: list[str] | None = None
        context_from_rag = False
        should_escalate = False
        uncertainty_reason: str | None = None
        is_knowledge_intent = intent in (
            Intent.ADMIN_LOGISTICS,
            Intent.META_PROGRAM,
            Intent.UNKNOWN,
        )

        if forced_context:
            context_section = (
                "Relevant information from the TechGym knowledge base:\n"
                f"{forced_context}\n\n"
                "Use ONLY the above context to answer. "
                "If the context doesn't contain a clear answer, say so honestly.\n"
            )
            context_sources = _extract_forced_context_titles(forced_context)
            if not context_sources:
                context_sources = ["forced_context"]
        elif is_knowledge_intent:
            context_started = time.perf_counter()
            context_result = await self._fetch_knowledge_context(
                message_text,
                local_corrections=local_corrections,
                rag_chunks=rag_chunks,
            )
            context = context_result.context_text
            context_sources = context_result.source_titles
            context_from_rag = context_result.from_rag
            context_page_ids = context_result.source_page_ids
            if context_result.has_source_conflict:
                return ConversationResult(
                    response=(
                        "מצאתי מקורות עם מידע סותר לשאלה הזאת. "
                        "כדי לדייק, בחר/י עמוד/ים רלוונטיים מהאינדקס."
                    ),
                    intent=intent,
                    is_confident=False,
                    should_escalate=True,
                    needs_clarification=False,
                    clarification_question=None,
                    is_grounded=False,
                    source_count=len(context_sources),
                    uncertainty_reason="source_conflict",
                )
            self._log_latency(
                "knowledge_context_build",
                context_started,
                {
                    "intent": intent.value,
                    "has_context": bool(context),
                    "source_count": len(context_sources),
                },
            )
            if context:
                context_section = (
                    "Relevant information from the TechGym knowledge base:\n"
                    f"{context}\n\n"
                    "Use ONLY the above context to answer. "
                    "If the context doesn't contain a clear answer, say so honestly.\n"
                )
            else:
                context_section = (
                    "No relevant information was found in the knowledge base for this question.\n"
                    "Be honest that you don't have the answer and that you'll pass it on to the team.\n"
                )
                should_escalate = True
                uncertainty_reason = "no_source"
        elif intent == Intent.TEMPLATES:
            template_started = time.perf_counter()
            template_text = await self._fetch_template(event_type or "general")
            self._log_latency(
                "template_fetch",
                template_started,
                {"event_type": event_type or "general", "found": bool(template_text)},
            )
            if template_text:
                context_section = (
                    f"Here is the template the leader requested:\n{template_text}\n\n"
                    "Present this template naturally. The leader can copy and send it to their group.\n"
                )
            else:
                context_section = (
                    "No template was found for this request.\n"
                    "Let the leader know and offer to escalate.\n"
                )
                should_escalate = True

        elif intent == Intent.FACILITATION:
            context_section = (
                "IMPORTANT: This question is about facilitation, group dynamics, or interpersonal advice.\n"
                "You must NOT answer it. Warmly explain that you only handle administrative topics, "
                "and suggest raising it in the leaders group to get advice from other leaders.\n"
            )
            should_escalate = True

        group_info = ""
        if leader_group:
            group_info = f"- Group: {leader_group}"
        else:
            group_info = "- No group assigned (management/core team member)"

        gender_word = "זכר" if leader_gender == "masculine" else "נקבה"

        system = CONVERSATION_SYSTEM_PROMPT.format(
            leader_name=leader_name,
            leader_gender=gender_word,
            leader_role=leader_role or "—",
            intent_name=intent.value,
            group_info=group_info,
            context_section=context_section,
        )

        agent = Agent(
            model=self._settings.model_name,
            system_prompt=system,
            output_type=str,
        )
        answer_started = time.perf_counter()
        result = await agent.run(message_text)
        self._log_latency("answer_generation", answer_started, {"intent": intent.value})
        response = result.output.strip()
        if intent != Intent.GREETING:
            response = _strip_leading_greeting(response)
        response = _enforce_male_self_language(response)

        is_confident = (
            bool(response)
            and "אני לא בטוח" not in response
            and "לא מצאתי" not in response
            and "אין לי תשובה" not in response
        )
        is_grounded: bool | None = None
        needs_grounding_check = bool(
            context_section
            and is_knowledge_intent
            and not context_from_rag
        )
        if context_from_rag and context_section:
            is_grounded = True
        if needs_grounding_check:
            grounded_started = time.perf_counter()
            is_grounded = await self._validate_answer_grounding(
                question=message_text,
                answer=response,
                context_text=context_section,
                source_titles=context_sources,
            )
            self._log_latency(
                "grounding_validation",
                grounded_started,
                {
                    "intent": intent.value,
                    "is_grounded": is_grounded,
                    "source_count": len(context_sources),
                },
            )
            if not is_grounded:
                is_confident = False
                should_escalate = True
                uncertainty_reason = uncertainty_reason or "low_grounding"
                if forced_context:
                    response = (
                        "לא מצאתי תשובה ודאית מתוך העמודים שנבחרו. "
                        "בחר/י עמודים אחרים או עדכן/י שאלה מדויקת יותר."
                    )

        if is_knowledge_intent and _is_lecture_upload_site_question(
            _normalize_for_match(message_text)
        ):
            has_required_upload_source = _has_lecture_upload_source(context_sources)
            if not has_required_upload_source:
                is_confident = False
                should_escalate = True
                uncertainty_reason = "missing_upload_source"
                response = (
                    "כדי לדייק בתשובה הזאת, אני צריך שתבחר/י עמוד/ים רלוונטיים מהאינדקס."
                )

        needs_clarification = False
        clarification_question: str | None = None
        low_confidence_intents = (
            Intent.ADMIN_LOGISTICS,
            Intent.META_PROGRAM,
            Intent.TEMPLATES,
            Intent.UNKNOWN,
        )
        if not is_confident and intent in low_confidence_intents:
            if clarification_allowed:
                clarification_started = time.perf_counter()
                clarification_question = await self._generate_clarification_question(
                    original_question=message_text,
                    intent=intent,
                    context=context_section,
                )
                needs_clarification = bool(clarification_question)
                self._log_latency(
                    "clarification_generation",
                    clarification_started,
                    {"generated": needs_clarification, "intent": intent.value},
                )

            if needs_clarification:
                should_escalate = False
                response = clarification_question or response
            else:
                should_escalate = True
                if is_knowledge_intent and not uncertainty_reason:
                    uncertainty_reason = "needs_source_selection"

        output = ConversationResult(
            response=response,
            intent=intent,
            is_confident=is_confident,
            should_escalate=should_escalate,
            needs_clarification=needs_clarification,
            clarification_question=clarification_question,
            is_grounded=is_grounded,
            source_count=len(context_sources),
            uncertainty_reason=uncertainty_reason,
            source_titles=context_sources or None,
            source_page_ids=context_page_ids,
        )
        self._log_latency(
            "respond_total",
            response_started,
            {
                "intent": intent.value,
                "is_confident": output.is_confident,
                "needs_clarification": output.needs_clarification,
                "should_escalate": output.should_escalate,
                "is_grounded": output.is_grounded,
                "source_count": output.source_count,
                "uncertainty_reason": output.uncertainty_reason,
            },
        )
        return output

    # ------------------------------------------------------------------
    # Context fetching
    # ------------------------------------------------------------------

    async def _fetch_knowledge_context(
        self,
        question: str,
        local_corrections: list[dict[str, str]] | None = None,
        rag_chunks: list[dict[str, str]] | None = None,
    ) -> KnowledgeContextResult:
        """Fetch relevant content from FAQ and Guides databases."""
        context_parts: list[str] = []
        source_titles: list[str] = []
        normalized_question = _normalize_for_match(question)
        keywords = _extract_keywords(question)
        cycle_number = _extract_cycle_number(normalized_question)
        if _is_dates_cycle_question(normalized_question) and cycle_number:
            # First, resolve via Guides index rows (DB pages), not only standalone pages.
            if self._settings.notion_guides_db_id:
                try:
                    guide_pages = await self._notion.get_all_guide_pages(
                        self._settings.notion_guides_db_id
                    )
                    matched_rows = [
                        row
                        for row in guide_pages
                        if _is_cycle_dates_title(
                            _normalize_for_match(row.get("title", "")),
                            cycle_number,
                        )
                    ]
                    if matched_rows:
                        page_rows = await self._notion.get_guide_contents_strict(
                            [row["page_id"] for row in matched_rows],
                            titles=[row.get("title", "") for row in matched_rows],
                            source_urls=[row.get("source_url", "") for row in matched_rows],
                        )
                        for row in page_rows:
                            content = row.get("content", "").strip()
                            if not content:
                                continue
                            title = row.get("resolved_title") or "תאריכים מחזור"
                            context_parts.append(content[:5000])
                            source_titles.append(str(title))
                            return KnowledgeContextResult(
                                context_text="\n---\n".join(context_parts),
                                source_titles=_dedupe_strings(source_titles),
                            )
                except Exception:
                    logger.exception("Failed resolving cycle dates page from guides index")

            cycle_titles = (
                f"תאריכים מחזור {cycle_number}",
                f"לוז מחזור {cycle_number}",
                f"לו\"ז מחזור {cycle_number}",
            )
            for title in cycle_titles:
                resolved = await self._notion.get_page_content_by_exact_title(title)
                if not resolved:
                    continue
                context_parts.append(resolved["content"][:5000])
                source_titles.append(title)
                return KnowledgeContextResult(
                    context_text="\n---\n".join(context_parts),
                    source_titles=_dedupe_strings(source_titles),
                )

        if local_corrections:
            scored_corrections: list[tuple[int, dict[str, str]]] = []
            for row in local_corrections:
                score = _score_correction_match(question, row)
                if score > 0:
                    scored_corrections.append((score, row))
            scored_corrections.sort(key=lambda item: item[0], reverse=True)
            for _, row in scored_corrections[:3]:
                corrected = row.get("corrected_answer", "").strip()
                source_question = row.get("question_text", "").strip()
                if corrected:
                    context_parts.append(
                        "Human-validated correction:\n"
                        f"Original question: {source_question}\n"
                        f"Correct answer: {corrected}"
                    )
                    source_titles.append("local_correction")

        has_registration_selection_intent = _contains_any(
            normalized_question,
            (
                "נרשמו",
                "הרשמה",
                "רישום",
                "רשומים",
                "נסגרה ההרשמה",
                "לבחור",
                "בחירה",
                "מיון",
                "סינון",
                "לקבוצה",
                "קבוצה",
            ),
        )
        faq_task = None
        guide_docs_task = None
        if self._settings.notion_faq_db_id:
            faq_task = asyncio.create_task(
                self._notion.get_faq_entries(self._settings.notion_faq_db_id)
            )
        if self._settings.notion_guides_db_id:
            guide_docs_task = asyncio.create_task(
                self._notion.get_all_guide_documents(self._settings.notion_guides_db_id)
            )

        faq_entries = []
        all_docs: list[dict[str, str]] = []
        if faq_task or guide_docs_task:
            results = await asyncio.gather(
                faq_task if faq_task else _completed_empty_list(),
                guide_docs_task if guide_docs_task else _completed_empty_list(),
                return_exceptions=True,
            )
            faq_result, guide_result = results
            if isinstance(faq_result, Exception):
                logger.warning("FAQ DB not available, skipping")
            else:
                faq_entries = faq_result
            if isinstance(guide_result, Exception):
                logger.exception("Failed to search guides")
            else:
                all_docs = guide_result

        has_lecture_site_upload_intent = _is_lecture_upload_site_question(normalized_question)
        if all_docs and has_lecture_site_upload_intent:
            mandatory_docs = _find_mandatory_lecture_upload_docs(all_docs)
            if mandatory_docs:
                # Hard route: for this known question type, prefer only the
                # dedicated guide page(s) to avoid drift to unrelated channels.
                for doc in mandatory_docs:
                    context_parts.append(doc["content"][:4000])
                    source_titles.append(doc.get("title", "guide"))
                return KnowledgeContextResult(
                    context_text="\n---\n".join(context_parts),
                    source_titles=_dedupe_strings(source_titles),
                )

        scored_faq_entries: list[tuple[int, Any]] = []
        for entry in faq_entries:
            score = _score_faq_entry(
                question_norm=normalized_question,
                keywords=keywords,
                entry_question=entry.question,
                entry_answer=entry.answer,
                has_reference=bool(entry.answer_source_page_id),
            )
            if score > 0:
                scored_faq_entries.append((score, entry))

        scored_faq_entries.sort(key=lambda item: item[0], reverse=True)
        if scored_faq_entries:
            top_score, top_entry = scored_faq_entries[0]
            # If FAQ has a strong match and points to a referenced page, trust it
            # and avoid mixing unrelated contexts.
            if top_score >= 10 and top_entry.answer_source_page_id:
                context_parts.append(f"FAQ: {top_entry.question}\n{top_entry.answer}")
                source_titles.append(f"faq_ref:{top_entry.question[:80]}")
                return KnowledgeContextResult(
                    context_text="\n---\n".join(context_parts),
                    source_titles=_dedupe_strings(source_titles),
                )

            for _, entry in scored_faq_entries[:3]:
                context_parts.append(f"FAQ: {entry.question}\n{entry.answer}")
                source_titles.append(f"faq:{entry.question[:80]}")

        used_rag = False
        source_page_ids: list[str] = []
        if rag_chunks:
            # RAG path: use pre-retrieved semantic chunks.
            seen_titles: set[str] = set()
            seen_page_ids: set[str] = set()
            for chunk in rag_chunks:
                context_parts.append(chunk["chunk_text"])
                title = chunk.get("page_title", "guide")
                page_id = chunk.get("notion_page_id", "")
                if title not in seen_titles:
                    source_titles.append(title)
                    seen_titles.add(title)
                if page_id and page_id not in seen_page_ids:
                    source_page_ids.append(page_id)
                    seen_page_ids.add(page_id)
            used_rag = True
            logger.info("RAG path: using %d pre-retrieved chunks", len(rag_chunks))

        if not used_rag and all_docs:
            # Legacy path: LLM page selection + keyword fallback.
            all_guides = [
                {"title": d["title"], "page_id": d["page_id"]}
                for d in all_docs
            ]
            purposes = await self._ensure_page_purposes(all_docs)
            selection_started = time.perf_counter()
            selected = await self._select_relevant_pages(question, all_guides, purposes=purposes)
            self._log_latency(
                "guide_page_selection",
                selection_started,
                {"selected": len(selected), "available": len(all_guides)},
            )
            docs_by_id = {d["page_id"]: d for d in all_docs}
            included_page_ids: set[str] = set()
            for row in selected:
                doc = docs_by_id.get(row["page_id"])
                if not doc:
                    continue
                included_page_ids.add(doc["page_id"])
                context_parts.append(doc["content"][:2000])
                source_titles.append(doc.get("title", "guide"))

            scored_docs: list[tuple[int, dict[str, str]]] = []
            for doc in all_docs:
                if doc["page_id"] in included_page_ids:
                    continue
                haystack = _normalize_for_match(f"{doc['title']} {doc['content']}")
                score = sum(1 for kw in keywords if kw.lower() in haystack)
                if has_registration_selection_intent:
                    score += sum(
                        2
                        for marker in (
                            "הרשמה",
                            "רישום",
                            "סגירת הרשמה",
                            "לבחור",
                            "בחירת משתתפים",
                            "מיון",
                            "קבלה",
                            "קבוצה",
                        )
                        if marker in haystack
                    )
                if score > 0:
                    scored_docs.append((score, doc))

            scored_docs.sort(key=lambda x: x[0], reverse=True)
            fallback_limit = 4 if has_registration_selection_intent else 2
            for _, doc in scored_docs[:fallback_limit]:
                context_parts.append(doc["content"][:2000])
                source_titles.append(doc.get("title", "guide"))

        source_pairs = list(zip(source_titles, context_parts, strict=False))
        if not used_rag:
            reduced_pairs = _reduce_source_pairs_for_answer(
                question=question,
                source_pairs=source_pairs,
            )
        else:
            reduced_pairs = source_pairs
        if _has_source_conflict(normalized_question, reduced_pairs):
            return KnowledgeContextResult(
                context_text="",
                source_titles=[title for title, _ in reduced_pairs],
                has_source_conflict=True,
                from_rag=used_rag,
                source_page_ids=source_page_ids or None,
            )
        return KnowledgeContextResult(
            context_text="\n---\n".join(content for _, content in reduced_pairs),
            source_titles=_dedupe_strings([title for title, _ in reduced_pairs]),
            from_rag=used_rag,
            source_page_ids=source_page_ids or None,
        )

    async def _validate_answer_grounding(
        self,
        question: str,
        answer: str,
        context_text: str,
        source_titles: list[str],
    ) -> bool:
        if not context_text.strip():
            return False
        answer_overlap = _rough_overlap_score(answer, context_text)
        question_overlap = _rough_overlap_score(question, context_text)
        logger.info(
            "Grounding lexical scores: answer_overlap=%d question_overlap=%d",
            answer_overlap,
            question_overlap,
        )
        # Skip the expensive LLM call only when neither the answer nor the
        # original question share meaningful vocabulary with the context.
        if answer_overlap < 2 and question_overlap < 1:
            return False

        prompt = (
            f"Question:\n{question}\n\n"
            f"Source titles: {', '.join(source_titles) if source_titles else 'n/a'}\n\n"
            f"Knowledge context:\n{context_text[:8000]}\n\n"
            f"Assistant answer:\n{answer}\n\n"
            "Is the answer fully grounded in the knowledge context?"
        )
        try:
            agent = Agent(
                model=self._settings.model_name,
                system_prompt=GROUNDING_VALIDATION_SYSTEM_PROMPT,
                output_type=str,
            )
            result = await agent.run(prompt)
            verdict = result.output.strip().lower()
            return verdict == "grounded"
        except Exception:
            logger.exception("Grounding validation failed, falling back to lexical check")
            return _rough_overlap_score(answer, context_text) >= 4

    async def _fetch_template(self, event_type: str) -> str | None:
        if not self._settings.notion_templates_db_id:
            return None
        try:
            templates = await self._notion.get_templates_by_event_type(
                event_type, self._settings.notion_templates_db_id
            )
            if templates:
                return templates[0].template_text
        except Exception:
            logger.exception("Failed to fetch template")
        return None

    async def _generate_clarification_question(
        self, original_question: str, intent: Intent, context: str
    ) -> str | None:
        """Generate one focused clarification question when confidence is low."""
        prompt = (
            f"Intent: {intent.value}\n"
            f"Original question:\n{original_question}\n\n"
            "Knowledge context (may be empty or partial):\n"
            f"{context[:1200] if context else 'No context found.'}\n\n"
            "Write one best clarification question in Hebrew."
        )
        try:
            agent = Agent(
                model=self._settings.model_name,
                system_prompt=CLARIFICATION_SYSTEM_PROMPT,
                output_type=str,
            )
            result = await agent.run(prompt)
            question = result.output.strip()
            return question or None
        except Exception:
            logger.exception("Failed to generate clarification question")
            return None

    async def _select_relevant_pages(
        self,
        question: str,
        guides: list[dict[str, str]],
        purposes: dict[str, str] | None = None,
    ) -> list[dict[str, str]]:
        """Ask the LLM which guide pages are relevant to the question."""
        purposes = purposes or {}
        lines: list[str] = []
        for i, g in enumerate(guides):
            purpose = purposes.get(g.get("page_id", ""), "")
            if purpose:
                lines.append(f"{i+1}. {g['title']} — {purpose}")
            else:
                lines.append(f"{i+1}. {g['title']}")
        titles_list = "\n".join(lines)
        prompt = (
            f"Given this question from a TechGym group leader:\n"
            f'"{question}"\n\n'
            f"Here are the available knowledge-base pages:\n{titles_list}\n\n"
            f"Return ONLY the numbers of the pages most likely to contain "
            f"the answer (up to 5), comma-separated. "
            f"If none seem relevant, return 'none'."
        )
        agent = Agent(
            model=self._settings.model_name,
            system_prompt=PAGE_SELECTION_SYSTEM_PROMPT,
            output_type=str,
        )
        result = await agent.run(prompt)
        raw = result.output.strip()
        logger.info("Page selection for '%s': %s", question[:50], raw)

        if "none" in raw.lower():
            return []

        selected: list[dict[str, str]] = []
        for token in raw.replace(",", " ").split():
            token = token.strip().rstrip(".")
            if token.isdigit():
                idx = int(token) - 1
                if 0 <= idx < len(guides):
                    selected.append(guides[idx])
        return selected[:5]

    async def _extract_event_type(self, message_text: str) -> str:
        started = time.perf_counter()
        agent = Agent(
            model=self._settings.model_name,
            system_prompt=EVENT_TYPE_SYSTEM_PROMPT,
            output_type=str,
        )
        result = await agent.run(message_text)
        output = result.output.strip().lower()
        self._log_latency(
            "event_type_extract",
            started,
            {"event_type": output},
        )
        return output

    # ------------------------------------------------------------------
    # Page purpose metadata
    # ------------------------------------------------------------------

    async def _load_page_purposes(
        self, page_ids: list[str]
    ) -> dict[str, str]:
        """Load stored purposes from the local DB for the given page IDs."""
        if not self._session_factory or not page_ids:
            return {}
        from models.notion_page_meta import NotionPageMeta
        from sqlmodel import col, select

        try:
            async with self._session_factory() as session:
                stmt = select(NotionPageMeta).where(
                    col(NotionPageMeta.notion_page_id).in_(page_ids)
                )
                result = await session.exec(stmt)
                rows = result.all()
                return {
                    row.notion_page_id: row.purpose
                    for row in rows
                    if row.purpose
                }
        except Exception:
            logger.debug("Failed to load page purposes from DB")
            return {}

    async def _ensure_page_purposes(
        self, pages: list[dict[str, str]]
    ) -> dict[str, str]:
        """Return purposes for all pages, auto-generating missing ones.

        Returns a dict mapping page_id -> purpose.
        """
        if not pages:
            return {}

        page_ids = [p["page_id"] for p in pages]
        existing = await self._load_page_purposes(page_ids)

        missing = [p for p in pages if p["page_id"] not in existing]
        if not missing or not self._session_factory:
            return existing

        generated = await self._generate_purposes_batch(missing)
        if generated:
            await self._store_page_purposes(generated, pages)
            existing.update(
                {pid: purpose for pid, purpose in generated.items() if purpose}
            )
        return existing

    async def _generate_purposes_batch(
        self, pages: list[dict[str, str]]
    ) -> dict[str, str]:
        """Use LLM to generate purpose descriptions for multiple pages."""
        results: dict[str, str] = {}

        async def gen_one(page: dict[str, str]) -> tuple[str, str]:
            title = page.get("title", "")
            content = page.get("content", "")[:500]
            prompt = (
                f"Page title: {title}\n"
                f"Content preview:\n{content}\n\n"
                "Write a concise Hebrew description of this page."
            )
            try:
                agent = Agent(
                    model=self._settings.model_name,
                    system_prompt=PURPOSE_GENERATION_SYSTEM_PROMPT,
                    output_type=str,
                )
                result = await agent.run(prompt)
                return page["page_id"], result.output.strip()[:300]
            except Exception:
                logger.debug("Failed to generate purpose for page %s", page.get("page_id"))
                return page["page_id"], ""

        tasks = [gen_one(p) for p in pages[:30]]
        generated = await asyncio.gather(*tasks, return_exceptions=True)
        for item in generated:
            if isinstance(item, tuple):
                pid, purpose = item
                if purpose:
                    results[pid] = purpose
        return results

    async def _store_page_purposes(
        self,
        purposes: dict[str, str],
        pages: list[dict[str, str]],
    ) -> None:
        """Persist auto-generated purposes to the local DB."""
        if not self._session_factory or not purposes:
            return
        from datetime import datetime, timezone
        from models.notion_page_meta import NotionPageMeta

        title_map = {p["page_id"]: p.get("title", "") for p in pages}
        source_type_map = {p["page_id"]: p.get("source_type", "guide") for p in pages}

        try:
            async with self._session_factory() as session:
                for page_id, purpose in purposes.items():
                    from sqlmodel import select

                    stmt = select(NotionPageMeta).where(
                        NotionPageMeta.notion_page_id == page_id
                    )
                    result = await session.exec(stmt)
                    existing = result.first()
                    now = datetime.now(timezone.utc)
                    if existing:
                        if existing.is_auto_generated:
                            existing.purpose = purpose
                            existing.title = title_map.get(page_id, existing.title)
                            existing.updated_at = now
                            session.add(existing)
                    else:
                        row = NotionPageMeta(
                            notion_page_id=page_id,
                            title=title_map.get(page_id, ""),
                            purpose=purpose,
                            source_type=source_type_map.get(page_id, "guide"),
                            is_auto_generated=True,
                            created_at=now,
                            updated_at=now,
                        )
                        session.add(row)
                await session.commit()
        except Exception:
            logger.exception("Failed to store page purposes")

    async def refresh_all_purposes(self) -> int:
        """Regenerate all auto-generated purposes. Returns count updated."""
        if not self._settings.notion_guides_db_id or not self._session_factory:
            return 0

        all_docs = await self._notion.get_all_guide_documents(
            self._settings.notion_guides_db_id
        )
        if not all_docs:
            return 0

        for doc in all_docs:
            doc.setdefault("source_type", "guide")
        generated = await self._generate_purposes_batch(all_docs)
        if generated:
            await self._store_page_purposes(generated, all_docs)
        return len(generated)

    def _log_latency(self, stage: str, started: float, extra: dict | None = None) -> None:
        payload: dict = {
            "stage": stage,
            "latency_ms": round((time.perf_counter() - started) * 1000, 2),
        }
        if extra:
            payload.update(extra)
        _timing_events.append(payload)
        logger.info("jimmy_timing %s", payload)


def _extract_keywords(text: str) -> list[str]:
    """Extract simple Hebrew keywords from text for fuzzy matching."""
    stop_words = {
        "את", "של", "מה", "איך", "מתי", "לי", "זה", "אני", "יש",
        "לא", "כן", "גם", "או", "עם", "על", "בלי", "כל", "היא", "הוא", "הם",
    }
    normalized = _normalize_for_match(text)
    words = normalized.split()
    return [w for w in words if len(w) > 2 and w not in stop_words][:12]


def _heuristic_intent(message_text: str) -> ClassificationResult | None:
    """Fast-path overrides for high-confidence patterns that the LLM
    sometimes misclassifies.
    """
    text = message_text.strip().lower()
    compact = re.sub(r"\s+", " ", text)

    # Upload/process questions should be admin logistics, not templates.
    upload_markers = ("מעלים", "העלאה", "להעלות", "העלאת")
    site_markers = ("לאתר", "באתר", "site", "platform", "טכני")
    lecture_markers = ("הרצאה", "הרצאות", "סילבוס")
    if (
        any(m in compact for m in upload_markers)
        and any(m in compact for m in lecture_markers)
        and any(m in compact for m in site_markers)
    ):
        return ClassificationResult(intent=Intent.ADMIN_LOGISTICS)

    # Registration overflow / participant selection is administrative logistics.
    registration_markers = (
        "נרשמו",
        "הרשמה",
        "רישום",
        "נסגרה ההרשמה",
        "סגירת הרשמה",
    )
    selection_markers = (
        "לבחור",
        "בחירה",
        "מיון",
        "לסנן",
        "מי להכניס",
        "קבלה לקבוצה",
        "קבוצה",
    )
    overflow_markers = ("יותר מ", "מעל", "40", "ארבעים")
    if any(m in compact for m in registration_markers) and (
        any(m in compact for m in selection_markers)
        or any(m in compact for m in overflow_markers)
    ):
        return ClassificationResult(intent=Intent.ADMIN_LOGISTICS)

    # Explicit wording requests are template intent.
    template_markers = (
        "תנסח",
        "תכתוב לי",
        "מה לכתוב",
        "נוסח",
        "טיוטה",
        "הודעה לקבוצה",
        "הודעה לקבוצ",
    )
    if any(m in compact for m in template_markers):
        return ClassificationResult(intent=Intent.TEMPLATES, event_type="general")

    return None


async def _completed_empty_list() -> list:
    return []


def _normalize_for_match(text: str) -> str:
    lowered = text.lower()
    cleaned = re.sub(r"[^\w\s\u0590-\u05FF]", " ", lowered)
    return re.sub(r"\s+", " ", cleaned).strip()


def _contains_any(text: str, needles: tuple[str, ...]) -> bool:
    return any(needle in text for needle in needles)


def _score_correction_match(question: str, row: dict[str, str]) -> int:
    question_text = _normalize_for_match(question)
    original = _normalize_for_match(row.get("question_text", ""))
    corrected = _normalize_for_match(row.get("corrected_answer", ""))
    keywords = _extract_keywords(question)
    score = 0
    if original and (question_text in original or original in question_text):
        score += 10
    score += sum(2 for kw in keywords if kw in original)
    score += sum(1 for kw in keywords if kw in corrected)
    return score


_HEBREW_PREFIX_CHARS = "הבלמכשו"


def _strip_hebrew_prefix(word: str) -> str:
    """Strip one common Hebrew prefix (ה,ב,ל,מ,כ,ש,ו) from a word."""
    if len(word) > 3 and word[0] in _HEBREW_PREFIX_CHARS:
        return word[1:]
    return word


def _rough_overlap_score(answer: str, context_text: str) -> int:
    answer_tokens = set(_extract_keywords(answer))
    context_norm = _normalize_for_match(context_text)
    score = 0
    for token in answer_tokens:
        if token in context_norm:
            score += 1
        elif _strip_hebrew_prefix(token) in context_norm:
            score += 1
    return score


def _score_faq_entry(
    question_norm: str,
    keywords: list[str],
    entry_question: str,
    entry_answer: str,
    has_reference: bool,
) -> int:
    eq_norm = _normalize_for_match(entry_question)
    ea_norm = _normalize_for_match(entry_answer)
    score = 0
    if eq_norm and (eq_norm in question_norm or question_norm in eq_norm):
        score += 8
    score += sum(2 for kw in keywords if kw in eq_norm)
    score += sum(1 for kw in keywords if kw in ea_norm)
    if has_reference:
        score += 2
    return score


def _dedupe_strings(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = value.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _is_lecture_upload_site_question(normalized_question: str) -> bool:
    upload_markers = ("מעלים", "העלאה", "להעלות", "העלאת")
    lecture_markers = ("הרצאה", "הרצאות")
    site_markers = ("לאתר", "באתר", "אתר", "site", "platform")
    return (
        any(marker in normalized_question for marker in upload_markers)
        and any(marker in normalized_question for marker in lecture_markers)
        and any(marker in normalized_question for marker in site_markers)
    )


def _is_dates_cycle_question(normalized_question: str) -> bool:
    has_dates_marker = _contains_any(
        normalized_question,
        ("תאריכ", "לו ז", "לוז", "מועד", "דדליין", "זמנים"),
    )
    has_cycle_marker = _contains_any(
        normalized_question,
        ("מחזור", "cohort", "batch"),
    )
    return has_dates_marker and has_cycle_marker


def _extract_cycle_number(normalized_question: str) -> int | None:
    match = re.search(r"\b(\d{1,2})\b", normalized_question)
    if match:
        value = int(match.group(1))
        if 1 <= value <= 20:
            return value

    hebrew_ordinals = {
        "ראשון": 1,
        "שני": 2,
        "שלישי": 3,
        "רביעי": 4,
        "חמישי": 5,
        "שישי": 6,
        "שביעי": 7,
        "שמיני": 8,
        "תשיעי": 9,
        "עשירי": 10,
    }
    for word, value in hebrew_ordinals.items():
        if word in normalized_question:
            return value
    return None


def _is_cycle_dates_title(normalized_title: str, cycle_number: int) -> bool:
    if not normalized_title:
        return False
    has_dates = _contains_any(normalized_title, ("תאריכ", "לוז", "לו ז", "מועד", "זמנים"))
    if not has_dates:
        return False
    cycle_markers = (
        f"מחזור {cycle_number}",
        f"סבב {cycle_number}",
        f"cohort {cycle_number}",
        f"batch {cycle_number}",
    )
    if any(marker in normalized_title for marker in cycle_markers):
        return True
    return cycle_number == 5 and "חמישי" in normalized_title


def _find_mandatory_lecture_upload_docs(
    all_docs: list[dict[str, str]],
) -> list[dict[str, str]]:
    preferred_titles = (
        "העלאת הרצאות לאתר",
        "העלאה לאתר",
        "העלאת הרצאה לאתר",
    )
    normalized_docs = [
        (doc, _normalize_for_match(doc.get("title", "")))
        for doc in all_docs
    ]
    strict_matches: list[dict[str, str]] = []
    for doc, normalized_title in normalized_docs:
        if any(_normalize_for_match(title) == normalized_title for title in preferred_titles):
            strict_matches.append(doc)
    if strict_matches:
        return strict_matches

    soft_matches: list[dict[str, str]] = []
    for doc, normalized_title in normalized_docs:
        if (
            "העלא" in normalized_title
            and "הרצא" in normalized_title
            and "אתר" in normalized_title
        ):
            soft_matches.append(doc)
    return soft_matches


def _has_lecture_upload_source(source_titles: list[str]) -> bool:
    if not source_titles:
        return False
    for title in source_titles:
        normalized = _normalize_for_match(title)
        if (
            "העלא" in normalized
            and "הרצא" in normalized
            and "אתר" in normalized
        ):
            return True
    return False


def _extract_forced_context_titles(forced_context: str) -> list[str]:
    titles: list[str] = []
    for line in forced_context.splitlines():
        prefix = "Selected page title:"
        if not line.startswith(prefix):
            continue
        title = line[len(prefix):].strip()
        if title:
            titles.append(title)
    return _dedupe_strings(titles)


def _reduce_source_pairs_for_answer(
    question: str,
    source_pairs: list[tuple[str, str]],
    max_sources: int = 2,
) -> list[tuple[str, str]]:
    if not source_pairs:
        return []
    if len(source_pairs) == 1:
        return source_pairs

    scored: list[tuple[int, str, str]] = []
    for title, content in source_pairs:
        score = _rough_overlap_score(question, f"{title}\n{content}")
        scored.append((score, title, content))
    scored.sort(key=lambda item: item[0], reverse=True)

    top_score = scored[0][0]
    second_score = scored[1][0] if len(scored) > 1 else -1
    # Single-source-first: if one source is clearly dominant, use only it.
    if top_score >= max(4, second_score + 3):
        return [(scored[0][1], scored[0][2])]

    limited = scored[: max(1, max_sources)]
    return [(title, content) for _, title, content in limited]


def _has_source_conflict(
    normalized_question: str, source_pairs: list[tuple[str, str]]
) -> bool:
    if len(source_pairs) < 2:
        return False

    # High-risk contradiction family we repeatedly saw in production.
    if _is_lecture_upload_site_question(normalized_question):
        has_youtube = False
        has_site = False
        for _, content in source_pairs:
            normalized_content = _normalize_for_match(content)
            if "יוטיוב" in normalized_content or "youtube" in normalized_content:
                has_youtube = True
            if "לאתר" in normalized_content or "באתר" in normalized_content:
                has_site = True
        return has_youtube and has_site
    return False


def _strip_leading_greeting(text: str) -> str:
    """Remove greeting opener when intent is not GREETING."""
    if not text:
        return text
    patterns = [
        r"^\s*(היי|הי|שלום|אהלן|הולה|hey|hi)\s*[!,.:-]*\s*",
        r"^\s*(היי|הי|שלום|אהלן|הולה|hey|hi)\s+\S+\s*[!,.:-]*\s*",
    ]
    stripped = text
    for pat in patterns:
        stripped = re.sub(pat, "", stripped, flags=re.IGNORECASE)
    return stripped.strip() or text


def _enforce_male_self_language(text: str) -> str:
    """Normalize common first-person feminine forms to masculine."""
    if not text:
        return text
    replacements = (
        ("אני יכולה", "אני יכול"),
        ("אני לא יכולה", "אני לא יכול"),
        ("אני בטוחה", "אני בטוח"),
        ("אני לא בטוחה", "אני לא בטוח"),
        ("אשמח לעזור לך", "אשמח לעזור לך"),  # no-op keeps phrase list explicit
        ("ממליצה", "ממליץ"),
        ("שמחתי לעזור", "שמחתי לעזור"),  # neutral
    )
    normalized = text
    for src, dst in replacements:
        normalized = normalized.replace(src, dst)
    return normalized


def get_recent_timing_events(limit: int = 20) -> list[dict]:
    limit = max(1, min(limit, 200))
    return list(_timing_events)[-limit:]
