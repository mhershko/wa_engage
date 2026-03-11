"""Notion API client for Jimmy bot.

Wraps the official Notion REST API to query leader data, templates,
reminder rules, FAQ entries, and guide pages.
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Any

import httpx

logger = logging.getLogger(__name__)

NOTION_API_VERSION = "2022-06-28"
NOTION_BASE_URL = "https://api.notion.com/v1"


@dataclass
class LeaderRecord:
    notion_page_id: str
    name: str
    phone: str
    group_name: str | None
    meeting_weekday: str | None
    meeting_time: str | None
    gender: str | None = None  # "זכר" / "נקבה" (or English "male"/"female")
    role: str | None = None  # e.g. "מוביל/ה", "צוות ליבה", "מוביל/ה, צוות ליבה"

    @property
    def is_masculine(self) -> bool:
        if not self.gender:
            return False
        g = self.gender.strip().lower()
        return g in ("זכר", "male", "m", "ז")

    @property
    def is_feminine(self) -> bool:
        return not self.is_masculine

    @property
    def _role_lower(self) -> str:
        return self.role.lower() if self.role else ""

    @property
    def has_group(self) -> bool:
        """Only true when the role explicitly includes group-leader."""
        return bool(self.group_name) and any(
            kw in self._role_lower for kw in ("מוביל", "leader")
        )

    @property
    def is_management(self) -> bool:
        if not self.role:
            return not self.has_group
        return any(
            kw in self._role_lower
            for kw in ("ניהול", "ליבה", "management", "core", "צוות")
        )

    @property
    def is_leader(self) -> bool:
        return any(kw in self._role_lower for kw in ("מוביל", "leader"))


@dataclass
class TemplateRecord:
    notion_page_id: str
    event_type: str
    template_text: str


@dataclass
class ReminderRule:
    notion_page_id: str
    rule_type: str  # "weekly_meeting" or "global_event"
    enabled: bool
    offset: str | None  # e.g. "1_day_before", "3_hours_before"
    specific_datetime: str | None
    message_text: str | None
    template_event_type: str | None


@dataclass
class FAQEntry:
    notion_page_id: str
    question: str
    answer: str
    tags: list[str] = field(default_factory=list)


def _format_uuid(raw_id: str) -> str:
    """Ensure a Notion ID is formatted as a dashed UUID."""
    clean = raw_id.replace("-", "")
    if len(clean) == 32:
        return f"{clean[:8]}-{clean[8:12]}-{clean[12:16]}-{clean[16:20]}-{clean[20:]}"
    return raw_id


def _normalize_phone(raw: str) -> str:
    """Strip non-digit chars and ensure consistent format."""
    digits = re.sub(r"\D", "", raw)
    if digits.startswith("0") and len(digits) == 10:
        digits = "972" + digits[1:]
    return digits


def _extract_plain_text(rich_text_array: list[dict[str, Any]]) -> str:
    return "".join(rt.get("plain_text", "") for rt in rich_text_array)


def _extract_property(properties: dict[str, Any], name: str) -> str | None:
    prop = properties.get(name)
    if not prop:
        return None

    prop_type = prop.get("type")
    if prop_type == "title":
        return _extract_plain_text(prop.get("title", []))
    if prop_type == "rich_text":
        return _extract_plain_text(prop.get("rich_text", []))
    if prop_type == "phone_number":
        return prop.get("phone_number")
    if prop_type == "select":
        sel = prop.get("select")
        return sel.get("name") if sel else None
    if prop_type == "checkbox":
        return str(prop.get("checkbox", False))
    if prop_type == "date":
        d = prop.get("date")
        return d.get("start") if d else None
    if prop_type == "multi_select":
        return ",".join(s.get("name", "") for s in prop.get("multi_select", []))
    if prop_type == "number":
        return str(prop.get("number")) if prop.get("number") is not None else None
    return None


def _build_leader_record(
    page_id: str, props: dict[str, Any], phone: str
) -> LeaderRecord:
    return LeaderRecord(
        notion_page_id=page_id,
        name=_extract_property(props, "שם")
        or _extract_property(props, "name")
        or "",
        phone=phone,
        group_name=_extract_property(props, "קבוצה")
        or _extract_property(props, "שם קבוצה")
        or _extract_property(props, "group_name"),
        meeting_weekday=_extract_property(props, "יום מפגש")
        or _extract_property(props, "meeting_weekday"),
        meeting_time=_extract_property(props, "שעת מפגש")
        or _extract_property(props, "meeting_time"),
        gender=_extract_property(props, "מין")
        or _extract_property(props, "gender"),
        role=_extract_property(props, "תפקיד")
        or _extract_property(props, "role"),
    )


class NotionClient:
    def __init__(self, api_key: str):
        self._client = httpx.AsyncClient(
            base_url=NOTION_BASE_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Notion-Version": NOTION_API_VERSION,
                "Content-Type": "application/json",
            },
            timeout=httpx.Timeout(30.0),
        )

    async def close(self):
        await self._client.aclose()

    # ------------------------------------------------------------------
    # Generic helpers
    # ------------------------------------------------------------------

    async def query_database(
        self, db_id: str, filters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Query a Notion database, handling pagination."""
        db_id = _format_uuid(db_id)
        results: list[dict[str, Any]] = []
        body: dict[str, Any] = {}
        if filters:
            body["filter"] = filters
        has_more = True
        while has_more:
            resp = await self._client.post(f"/databases/{db_id}/query", json=body)
            resp.raise_for_status()
            data = resp.json()
            results.extend(data.get("results", []))
            has_more = data.get("has_more", False)
            if has_more:
                body["start_cursor"] = data.get("next_cursor")
        return results

    async def retrieve_page(self, page_id: str) -> dict[str, Any]:
        resp = await self._client.get(f"/pages/{_format_uuid(page_id)}")
        resp.raise_for_status()
        return resp.json()

    async def search(self, query: str) -> list[dict[str, Any]]:
        resp = await self._client.post("/search", json={"query": query})
        resp.raise_for_status()
        return resp.json().get("results", [])

    async def get_page_content(self, page_id: str) -> str:
        """Retrieve all block children of a page as plain text."""
        blocks: list[str] = []
        has_more = True
        start_cursor: str | None = None
        while has_more:
            params: dict[str, Any] = {"page_size": 100}
            if start_cursor:
                params["start_cursor"] = start_cursor
            resp = await self._client.get(f"/blocks/{_format_uuid(page_id)}/children", params=params)
            resp.raise_for_status()
            data = resp.json()
            for block in data.get("results", []):
                btype = block.get("type", "")
                block_data = block.get(btype, {})
                if "rich_text" in block_data:
                    text = _extract_plain_text(block_data["rich_text"])
                    if text:
                        blocks.append(text)
            has_more = data.get("has_more", False)
            start_cursor = data.get("next_cursor")
        return "\n".join(blocks)

    # ------------------------------------------------------------------
    # Domain-specific queries
    # ------------------------------------------------------------------

    async def get_leader_by_phone(self, phone: str, db_id: str) -> LeaderRecord | None:
        normalized = _normalize_phone(phone)
        pages = await self.query_database(db_id)
        logger.debug(
            "Looking for phone %s (normalized: %s) in %d leader rows",
            phone, normalized, len(pages),
        )
        for page in pages:
            props = page.get("properties", {})
            raw_phone = _extract_property(props, "Phone") or _extract_property(
                props, "טלפון"
            ) or _extract_property(props, "phone")
            if not raw_phone:
                logger.debug("Row %s has no phone property, available: %s", page["id"], list(props.keys()))
                continue
            if _normalize_phone(raw_phone) == normalized:
                return _build_leader_record(page["id"], props, normalized)
        return None

    async def get_all_leaders(self, db_id: str) -> list[LeaderRecord]:
        pages = await self.query_database(db_id)
        leaders: list[LeaderRecord] = []
        for page in pages:
            props = page.get("properties", {})
            raw_phone = _extract_property(props, "Phone") or _extract_property(
                props, "טלפון"
            ) or _extract_property(props, "phone")
            if not raw_phone:
                continue
            leaders.append(_build_leader_record(page["id"], props, _normalize_phone(raw_phone)))
        return leaders

    async def get_templates_by_event_type(
        self, event_type: str, db_id: str
    ) -> list[TemplateRecord]:
        pages = await self.query_database(
            db_id,
            filters={
                "property": "event_type",
                "select": {"equals": event_type},
            },
        )
        templates: list[TemplateRecord] = []
        for page in pages:
            props = page.get("properties", {})
            text = _extract_property(props, "template_text") or ""
            if not text:
                text = await self.get_page_content(page["id"])
            templates.append(
                TemplateRecord(
                    notion_page_id=page["id"],
                    event_type=event_type,
                    template_text=text,
                )
            )
        return templates

    async def get_reminder_rules(self, db_id: str) -> list[ReminderRule]:
        pages = await self.query_database(db_id)
        rules: list[ReminderRule] = []
        for page in pages:
            props = page.get("properties", {})
            enabled_str = _extract_property(props, "enabled")
            enabled = enabled_str and enabled_str.lower() == "true"
            rules.append(
                ReminderRule(
                    notion_page_id=page["id"],
                    rule_type=_extract_property(props, "type") or "weekly_meeting",
                    enabled=enabled,
                    offset=_extract_property(props, "offset"),
                    specific_datetime=_extract_property(props, "specific_datetime"),
                    message_text=_extract_property(props, "message_text"),
                    template_event_type=_extract_property(props, "template_event_type"),
                )
            )
        return rules

    async def get_faq_entries(self, db_id: str) -> list[FAQEntry]:
        pages = await self.query_database(db_id)
        entries: list[FAQEntry] = []
        for page in pages:
            props = page.get("properties", {})
            question = (
                _extract_property(props, "question")
                or _extract_property(props, "שאלה")
                or ""
            )
            answer = (
                _extract_property(props, "answer")
                or _extract_property(props, "תשובה")
                or ""
            )
            if not answer:
                answer = await self.get_page_content(page["id"])
            tags_raw = _extract_property(props, "tags") or ""
            entries.append(
                FAQEntry(
                    notion_page_id=page["id"],
                    question=question,
                    answer=answer,
                    tags=[t.strip() for t in tags_raw.split(",") if t.strip()],
                )
            )
        return entries

    async def get_all_guide_pages(self, db_id: str) -> list[dict[str, str]]:
        """Return all pages in the guides DB with their title and page_id."""
        pages = await self.query_database(db_id)
        guides: list[dict[str, str]] = []
        for page in pages:
            props = page.get("properties", {})
            title = ""
            for prop in props.values():
                if prop.get("type") == "title":
                    title = _extract_plain_text(prop.get("title", []))
                    break
            if title:
                guides.append({"title": title, "page_id": page["id"]})
        return guides

    async def get_guide_contents(self, page_ids: list[str], titles: list[str] | None = None) -> list[dict[str, str]]:
        """Fetch content for guide pages. If a DB entry is empty, search for a
        standalone page with the same title that has actual content."""
        results: list[dict[str, str]] = []
        for i, pid in enumerate(page_ids):
            content = await self.get_page_content(pid)
            if not content and titles and i < len(titles):
                content = await self._find_content_page_by_title(titles[i])
            if content:
                results.append({"page_id": pid, "content": content})
        return results

    async def _find_content_page_by_title(self, title: str) -> str:
        """Search for a standalone page by title and return its content."""
        resp = await self._client.post("/search", json={"query": title})
        resp.raise_for_status()
        for page in resp.json().get("results", []):
            if page.get("object") != "page":
                continue
            parent_type = page.get("parent", {}).get("type", "")
            if parent_type == "database_id":
                continue
            content = await self.get_page_content(page["id"])
            if content:
                logger.info("Found content page for '%s': %s (%d chars)", title, page["id"], len(content))
                return content
        return ""

    async def get_admin_tasks(self, db_id: str) -> list[dict[str, str]]:
        pages = await self.query_database(db_id)
        tasks: list[dict[str, str]] = []
        for page in pages:
            props = page.get("properties", {})
            title = ""
            for prop in props.values():
                if prop.get("type") == "title":
                    title = _extract_plain_text(prop.get("title", []))
                    break
            tasks.append({"title": title, "page_id": page["id"]})
        return tasks
