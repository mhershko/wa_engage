"""JimmyBrain – conversational LLM-based assistant.

Every message goes through the LLM. The brain:
1. Classifies intent in the background (for escalation/routing decisions)
2. Fetches Notion context when needed
3. Generates a natural, conversational Hebrew response
"""

import enum
import logging
import re
from dataclasses import dataclass

from pydantic_ai import Agent

from config import Settings
from jimmy.notion_client import NotionClient

logger = logging.getLogger(__name__)


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


class JimmyBrain:
    def __init__(self, settings: Settings, notion: NotionClient):
        self._settings = settings
        self._notion = notion

    async def classify_intent(self, message_text: str) -> ClassificationResult:
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
    ) -> ConversationResult:
        """Generate a natural conversational response, fetching Notion context when needed."""

        context_section = ""
        should_escalate = False

        if intent in (Intent.ADMIN_LOGISTICS, Intent.META_PROGRAM, Intent.UNKNOWN):
            context = await self._fetch_knowledge_context(message_text)
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

        elif intent == Intent.TEMPLATES:
            template_text = await self._fetch_template(event_type or "general")
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
        result = await agent.run(message_text)
        response = result.output.strip()
        if intent != Intent.GREETING:
            response = _strip_leading_greeting(response)

        is_confident = (
            bool(response)
            and "אני לא בטוח" not in response
            and "לא מצאתי" not in response
            and "אין לי תשובה" not in response
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
                clarification_question = await self._generate_clarification_question(
                    original_question=message_text,
                    intent=intent,
                    context=context_section,
                )
                needs_clarification = bool(clarification_question)

            if needs_clarification:
                should_escalate = False
                response = clarification_question or response
            else:
                should_escalate = True

        return ConversationResult(
            response=response,
            intent=intent,
            is_confident=is_confident,
            should_escalate=should_escalate,
            needs_clarification=needs_clarification,
            clarification_question=clarification_question,
        )

    # ------------------------------------------------------------------
    # Context fetching
    # ------------------------------------------------------------------

    async def _fetch_knowledge_context(self, question: str) -> str:
        """Fetch relevant content from FAQ and Guides databases."""
        context_parts: list[str] = []

        if self._settings.notion_faq_db_id:
            try:
                keywords = _extract_keywords(question)
                faq_entries = await self._notion.get_faq_entries(
                    self._settings.notion_faq_db_id
                )
                for entry in faq_entries:
                    if any(
                        kw in entry.question.lower() or kw in entry.answer.lower()
                        for kw in keywords
                    ):
                        context_parts.append(f"FAQ: {entry.question}\n{entry.answer}")
            except Exception:
                logger.warning("FAQ DB not available, skipping")

        if self._settings.notion_guides_db_id:
            try:
                all_guides = await self._notion.get_all_guide_pages(
                    self._settings.notion_guides_db_id
                )
                if all_guides:
                    selected = await self._select_relevant_pages(
                        question, all_guides
                    )
                    if selected:
                        sel_ids = [s["page_id"] for s in selected]
                        sel_titles = [s["title"] for s in selected]
                        contents = await self._notion.get_guide_contents(
                            sel_ids, titles=sel_titles
                        )
                        for item in contents:
                            context_parts.append(item["content"][:2000])
            except Exception:
                logger.exception("Failed to search guides")

        return "\n---\n".join(context_parts)

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
        self, question: str, guides: list[dict[str, str]]
    ) -> list[dict[str, str]]:
        """Ask the LLM which guide pages are relevant to the question."""
        titles_list = "\n".join(
            f"{i+1}. {g['title']}" for i, g in enumerate(guides)
        )
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
        agent = Agent(
            model=self._settings.model_name,
            system_prompt=EVENT_TYPE_SYSTEM_PROMPT,
            output_type=str,
        )
        result = await agent.run(message_text)
        return result.output.strip().lower()


def _extract_keywords(text: str) -> list[str]:
    """Extract simple Hebrew keywords from text for fuzzy matching."""
    stop_words = {
        "את", "של", "מה", "איך", "מתי", "לי", "זה", "אני", "יש",
        "לא", "כן", "גם", "או", "עם", "על", "בלי", "כל", "היא", "הוא", "הם",
    }
    words = text.split()
    return [w for w in words if len(w) > 2 and w not in stop_words][:10]


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
