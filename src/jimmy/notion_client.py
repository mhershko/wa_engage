"""Notion API client for Jimmy bot.

Wraps the official Notion REST API to query leader data, templates,
reminder rules, FAQ entries, and guide pages.
"""

import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any

from cachetools import TTLCache
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
        return _resolve_gender(self.gender) == "masculine"

    @property
    def is_feminine(self) -> bool:
        return _resolve_gender(self.gender) == "feminine"

    @property
    def gender_kind(self) -> str | None:
        return _resolve_gender(self.gender)

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
    answer_source_page_id: str | None = None
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
    if prop_type == "url":
        return prop.get("url")
    return None


def _extract_property_ci(properties: dict[str, Any], name: str) -> str | None:
    """Case-insensitive property lookup."""
    direct = _extract_property(properties, name)
    if direct:
        return direct
    target = name.strip().lower()
    for key in properties:
        if key.strip().lower() != target:
            continue
        value = _extract_property(properties, key)
        if value:
            return value
    return None


def _extract_source_url_from_properties(properties: dict[str, Any]) -> str:
    candidates = (
        "page_link",
        "page url",
        "page link",
        "url",
        "link",
        "קישור",
    )
    for name in candidates:
        value = _extract_property_ci(properties, name)
        if value and value.strip():
            return value.strip()

    for prop in properties.values():
        prop_type = prop.get("type")
        if prop_type in {"rich_text", "title"}:
            arr = prop.get(prop_type, [])
            for item in arr:
                text_link = ((item.get("text") or {}).get("link") or {}).get("url")
                if text_link:
                    return text_link
                mention = item.get("mention") or {}
                link_preview = mention.get("link_preview") or {}
                preview_url = link_preview.get("url")
                if preview_url:
                    return preview_url
                if mention.get("type") == "database":
                    db_id = (mention.get("database") or {}).get("id")
                    if db_id:
                        return _format_uuid(db_id)
                if mention.get("type") == "page":
                    page_id = (mention.get("page") or {}).get("id")
                    if page_id:
                        return _format_uuid(page_id)
                href = item.get("href")
                if href:
                    return href

    # Scan every property whose Notion field type is "url" regardless of its name.
    for prop in properties.values():
        if prop.get("type") == "url" and prop.get("url"):
            return prop["url"].strip()

    # Last resort: follow the first page in any relation property.
    for prop in properties.values():
        if prop.get("type") == "relation":
            for rel in prop.get("relation", []):
                page_id = rel.get("id")
                if page_id:
                    return _format_uuid(page_id)

    return ""


def _extract_page_id_from_notion_url(raw_url: str | None) -> str | None:
    if not raw_url:
        return None
    match = re.search(r"([0-9a-fA-F]{32})", raw_url.replace("-", ""))
    if not match:
        return None
    return _format_uuid(match.group(1))


def _extract_reference_page_ids(properties: dict[str, Any]) -> list[str]:
    """Extract referenced Notion page IDs from FAQ row properties.

    Supports relation properties, URL properties that point to Notion pages,
    and rich_text mentions/links to Notion pages.
    """
    preferred_property_names = (
        "Page Reference",
        "page reference",
        "הפניה לעמוד",
    )
    preferred_keys = (
        "reference",
        "ref",
        "answer_page",
        "source_page",
        "page",
        "link",
        "מקור",
        "הפניה",
        "רפרנס",
        "עמוד",
    )
    ordered: list[tuple[str, dict[str, Any]]] = []
    used_names: set[str] = set()
    for name in preferred_property_names:
        if name in properties:
            ordered.append((name, properties[name]))
            used_names.add(name)

    remaining = sorted(
        (
            (name, prop)
            for name, prop in properties.items()
            if name not in used_names
        ),
        key=lambda kv: (
            0 if any(token in kv[0].strip().lower() for token in preferred_keys) else 1,
            kv[0],
        ),
    )
    ordered.extend(remaining)

    page_ids: list[str] = []
    for _, prop in ordered:
        prop_type = prop.get("type")
        if prop_type == "relation":
            for rel in prop.get("relation", []):
                page_id = rel.get("id")
                if page_id:
                    page_ids.append(_format_uuid(page_id))
        elif prop_type == "url":
            page_id = _extract_page_id_from_notion_url(prop.get("url"))
            if page_id:
                page_ids.append(page_id)
        elif prop_type in {"rich_text", "title"}:
            chunks = prop.get("rich_text", []) if prop_type == "rich_text" else prop.get("title", [])
            for chunk in chunks:
                mention = chunk.get("mention", {})
                if mention.get("type") == "page":
                    mention_page = mention.get("page", {}).get("id")
                    if mention_page:
                        page_ids.append(_format_uuid(mention_page))
                href = chunk.get("href")
                page_id = _extract_page_id_from_notion_url(href)
                if page_id:
                    page_ids.append(page_id)

    # Deduplicate while preserving order.
    unique_ids: list[str] = []
    seen: set[str] = set()
    for pid in page_ids:
        if pid in seen:
            continue
        seen.add(pid)
        unique_ids.append(pid)
    return unique_ids


def _resolve_gender(raw: str | None) -> str | None:
    """Map free-text/select gender values to canonical masculine/feminine."""
    if not raw:
        return None
    value = raw.strip().lower()
    masculine_tokens = {
        "זכר",
        "ז",
        "male",
        "m",
        "man",
        "boy",
        "בן",
        "גבר",
    }
    feminine_tokens = {
        "נקבה",
        "נ",
        "female",
        "f",
        "woman",
        "girl",
        "בת",
        "אישה",
    }
    if value in masculine_tokens:
        return "masculine"
    if value in feminine_tokens:
        return "feminine"
    if any(tok in value for tok in ("זכר", "male", "בן", "גבר")):
        return "masculine"
    if any(tok in value for tok in ("נקבה", "female", "בת", "אישה")):
        return "feminine"
    return None


def _normalize_for_match(text: str) -> str:
    lowered = text.lower()
    cleaned = re.sub(r"[^\w\s\u0590-\u05FF]", " ", lowered)
    return re.sub(r"\s+", " ", cleaned).strip()


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


def _extract_image_url(block_data: dict[str, Any]) -> str:
    """Extract downloadable URL from a Notion image block."""
    for source_key in ("file", "external"):
        source = block_data.get(source_key, {})
        url = source.get("url", "")
        if url:
            return url
    return ""


_IMAGE_EXTRACT_PROMPT = (
    "Extract ALL text and information visible in this image. "
    "Write the result in Hebrew. If the image contains a table or schedule, "
    "reproduce its content as structured text. Be concise but complete."
)


class NotionClient:
    def __init__(self, api_key: str, anthropic_api_key: str = ""):
        self._client = httpx.AsyncClient(
            base_url=NOTION_BASE_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Notion-Version": NOTION_API_VERSION,
                "Content-Type": "application/json",
            },
            timeout=httpx.Timeout(30.0),
        )
        self._anthropic_api_key = anthropic_api_key
        self._http_semaphore = asyncio.Semaphore(8)
        self._content_semaphore = asyncio.Semaphore(5)
        self._vision_semaphore = asyncio.Semaphore(3)
        self._cache_lock = asyncio.Lock()
        self._guide_pages_cache: TTLCache[str, list[dict[str, str]]] = TTLCache(
            maxsize=16, ttl=3600
        )
        self._guide_docs_cache: TTLCache[str, list[dict[str, str]]] = TTLCache(
            maxsize=8, ttl=3600
        )
        self._faq_cache: TTLCache[str, list[FAQEntry]] = TTLCache(
            maxsize=16, ttl=300
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
            resp = await self._post(f"/databases/{db_id}/query", json=body)
            resp.raise_for_status()
            data = resp.json()
            results.extend(data.get("results", []))
            has_more = data.get("has_more", False)
            if has_more:
                body["start_cursor"] = data.get("next_cursor")
        return results

    async def retrieve_page(self, page_id: str) -> dict[str, Any]:
        resp = await self._get(f"/pages/{_format_uuid(page_id)}")
        resp.raise_for_status()
        return resp.json()

    async def search(self, query: str) -> list[dict[str, Any]]:
        resp = await self._post("/search", json={"query": query})
        resp.raise_for_status()
        return resp.json().get("results", [])

    async def get_page_content(
        self, page_id: str, *, extract_images: bool = False
    ) -> str:
        """Retrieve block text recursively (including captions and child databases).

        Falls back to querying the ID as a database if the blocks API fails
        (e.g. when the ID points to a Notion database rather than a page).

        Set *extract_images* to True during indexing to run Claude Vision on
        image blocks.  Leave False for normal question-answering flows.
        """
        try:
            content = await self._collect_block_texts(
                _format_uuid(page_id), extract_images=extract_images
            )
        except httpx.HTTPStatusError:
            content = ""
        if not content:
            content = await self._get_database_as_text(page_id)
        return content

    async def _collect_block_texts(
        self, block_id: str, *, extract_images: bool = False
    ) -> str:
        chunks: list[str] = []
        has_more = True
        start_cursor: str | None = None
        while has_more:
            params: dict[str, Any] = {"page_size": 100}
            if start_cursor:
                params["start_cursor"] = start_cursor
            resp = await self._get(f"/blocks/{_format_uuid(block_id)}/children", params=params)
            resp.raise_for_status()
            data = resp.json()
            for block in data.get("results", []):
                btype = block.get("type", "")
                block_data = block.get(btype, {})
                if "rich_text" in block_data:
                    text = _extract_plain_text(block_data.get("rich_text", []))
                    if text:
                        chunks.append(text)
                if "caption" in block_data:
                    caption = _extract_plain_text(block_data.get("caption", []))
                    if caption:
                        chunks.append(caption)
                if btype == "table_row":
                    row_cells = [
                        _extract_plain_text(cell)
                        for cell in block_data.get("cells", [])
                    ]
                    row_text = " | ".join(c for c in row_cells if c)
                    if row_text:
                        chunks.append(row_text)
                if btype == "image" and extract_images:
                    image_url = _extract_image_url(block_data)
                    if image_url:
                        description = await self._describe_image(image_url)
                        if description:
                            chunks.append(description)
                if btype == "child_database":
                    db_content = await self._get_database_as_text(block["id"])
                    if db_content:
                        chunks.append(db_content)
                elif block.get("has_children"):
                    nested = await self._collect_block_texts(
                        block.get("id", ""), extract_images=extract_images
                    )
                    if nested:
                        chunks.append(nested)
            has_more = data.get("has_more", False)
            start_cursor = data.get("next_cursor")
        return "\n".join(part for part in chunks if part)

    async def _get_database_as_text(self, db_id: str) -> str:
        """Query a Notion database and format its rows as readable text."""
        try:
            pages = await self.query_database(db_id)
        except Exception:
            logger.debug("ID %s is not a queryable database", db_id)
            return ""

        if not pages:
            return ""

        rows: list[str] = []
        for page in pages:
            props = page.get("properties", {})
            title_part = ""
            other_parts: list[str] = []
            for prop_name, prop in props.items():
                value = _extract_property(props, prop_name)
                if not value:
                    continue
                if prop.get("type") == "title":
                    title_part = value
                else:
                    other_parts.append(f"{prop_name}: {value}")
            parts = ([title_part] if title_part else []) + other_parts
            if parts:
                rows.append(" | ".join(parts))

        return "\n".join(rows)

    async def _describe_image(self, image_url: str) -> str:
        """Use Claude Vision to extract text/info from an image."""
        if not self._anthropic_api_key:
            return ""
        async with self._vision_semaphore:
            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    resp = await client.post(
                        "https://api.anthropic.com/v1/messages",
                        headers={
                            "x-api-key": self._anthropic_api_key,
                            "anthropic-version": "2023-06-01",
                            "Content-Type": "application/json",
                        },
                        json={
                            "model": "claude-sonnet-4-5-20250929",
                            "max_tokens": 1024,
                            "messages": [
                                {
                                    "role": "user",
                                    "content": [
                                        {
                                            "type": "image",
                                            "source": {
                                                "type": "url",
                                                "url": image_url,
                                            },
                                        },
                                        {
                                            "type": "text",
                                            "text": _IMAGE_EXTRACT_PROMPT,
                                        },
                                    ],
                                }
                            ],
                        },
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    text_parts = [
                        block.get("text", "")
                        for block in data.get("content", [])
                        if block.get("type") == "text"
                    ]
                    result = "\n".join(text_parts).strip()
                    if result:
                        logger.info(
                            "Image described: %s (%d chars)",
                            image_url[:80],
                            len(result),
                        )
                    return result
            except Exception:
                logger.warning("Failed to describe image: %s", image_url[:80], exc_info=True)
                return ""

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
        async with self._cache_lock:
            cached = self._faq_cache.get(db_id)
        if cached is not None:
            return cached

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
            answer_source_page_id: str | None = None
            reference_page_ids = _extract_reference_page_ids(props)
            for ref_page_id in reference_page_ids:
                ref_content = await self.get_page_content(ref_page_id)
                if ref_content:
                    logger.info(
                        "FAQ page reference resolved: faq_row=%s source_page=%s chars=%d",
                        page["id"],
                        ref_page_id,
                        len(ref_content),
                    )
                    answer = ref_content
                    answer_source_page_id = ref_page_id
                    break

            if not answer:
                answer = await self.get_page_content(page["id"])
            tags_raw = _extract_property(props, "tags") or ""
            entries.append(
                FAQEntry(
                    notion_page_id=page["id"],
                    question=question,
                    answer=answer,
                    answer_source_page_id=answer_source_page_id,
                    tags=[t.strip() for t in tags_raw.split(",") if t.strip()],
                )
            )
        async with self._cache_lock:
            self._faq_cache[db_id] = entries
        return entries

    async def get_all_guide_pages(self, db_id: str) -> list[dict[str, str]]:
        """Return all pages in the guides DB with their title and page_id."""
        async with self._cache_lock:
            cached = self._guide_pages_cache.get(db_id)
        if cached is not None:
            return cached

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
                source_url = _extract_source_url_from_properties(props)
                logger.debug(
                    "Guide page '%s' page_id=%s source_url=%r",
                    title,
                    page["id"],
                    source_url,
                )
                guides.append(
                    {
                        "title": title,
                        "page_id": page["id"],
                        "source_url": source_url,
                    }
                )
        async with self._cache_lock:
            self._guide_pages_cache[db_id] = guides
        return guides

    async def get_guide_contents(self, page_ids: list[str], titles: list[str] | None = None) -> list[dict[str, str]]:
        """Fetch content for guide pages. If a DB entry is empty, search for a
        standalone page with the same title that has actual content."""
        async def fetch_one(i: int, pid: str) -> dict[str, str] | None:
            async with self._content_semaphore:
                content = await self.get_page_content(pid)
                if not content and titles and i < len(titles):
                    content = await self._find_content_page_by_title(titles[i])
                if content:
                    return {"page_id": pid, "content": content}
                return None

        rows = await asyncio.gather(
            *(fetch_one(i, pid) for i, pid in enumerate(page_ids)),
            return_exceptions=False,
        )
        return [row for row in rows if row]

    async def get_guide_contents_strict(
        self,
        page_ids: list[str],
        titles: list[str] | None = None,
        source_urls: list[str] | None = None,
    ) -> list[dict[str, str]]:
        """Fetch selected page content with deterministic exact-title fallback."""

        async def fetch_one(i: int, pid: str) -> dict[str, str] | None:
            async with self._content_semaphore:
                content = await self.get_page_content(pid)
                resolved_page_id = pid
                resolved_title = titles[i] if titles and i < len(titles) else ""
                if not content and source_urls and i < len(source_urls):
                    source_url = (source_urls[i] or "").strip()
                    linked_page_id = _extract_page_id_from_notion_url(source_url)
                    if linked_page_id and linked_page_id != pid:
                        linked_content = await self.get_page_content(linked_page_id)
                        if linked_content:
                            logger.info(
                                "Strict guide content resolved from source URL: requested=%s linked=%s title='%s' chars=%d",
                                pid,
                                linked_page_id,
                                resolved_title,
                                len(linked_content),
                            )
                            resolved_page_id = linked_page_id
                            content = linked_content
                if not content and titles and i < len(titles):
                    resolved = await self._find_content_page_by_title_exact(titles[i])
                    if resolved:
                        resolved_page_id, content = resolved
                if content:
                    logger.info(
                        "Strict guide content resolved: requested=%s resolved=%s title='%s' chars=%d",
                        pid,
                        resolved_page_id,
                        resolved_title,
                        len(content),
                    )
                    return {
                        "page_id": pid,
                        "resolved_page_id": resolved_page_id,
                        "resolved_title": resolved_title,
                        "content": content,
                    }
                return None

        rows = await asyncio.gather(
            *(fetch_one(i, pid) for i, pid in enumerate(page_ids)),
            return_exceptions=False,
        )
        return [row for row in rows if row]

    async def get_page_content_by_exact_title(
        self, title: str
    ) -> dict[str, str] | None:
        """Fetch standalone page content by exact title match."""
        resolved = await self._find_content_page_by_title_exact(title)
        if not resolved:
            return None
        page_id, content = resolved
        return {"page_id": page_id, "title": title, "content": content}

    async def _find_content_page_by_title_exact(self, title: str) -> tuple[str, str] | None:
        """Find a standalone page whose title exactly matches the given title."""
        resp = await self._post("/search", json={"query": title})
        resp.raise_for_status()
        target = _normalize_for_match(title)
        candidates: list[tuple[str, str]] = []
        for page in resp.json().get("results", []):
            if page.get("object") != "page":
                continue
            page_title = self._extract_page_title_from_search_result(page)
            if page_title and _normalize_for_match(page_title) == target:
                candidates.append((page["id"], page_title))
        for page_id, _ in candidates:
            content = await self.get_page_content(page_id)
            if content:
                logger.info(
                    "Found exact content page for '%s': %s (%d chars)",
                    title,
                    page_id,
                    len(content),
                )
                return page_id, content
        return None

    def _extract_page_title_from_search_result(self, page: dict[str, Any]) -> str:
        props = page.get("properties", {})
        for prop in props.values():
            if prop.get("type") == "title":
                return _extract_plain_text(prop.get("title", []))
        return ""

    async def get_all_guide_documents(self, db_id: str) -> list[dict[str, str]]:
        """Return guide documents with title + content for retrieval ranking."""
        async with self._cache_lock:
            cached = self._guide_docs_cache.get(db_id)
        if cached is not None:
            return cached

        guides = await self.get_all_guide_pages(db_id)
        if not guides:
            return []
        page_ids = [g["page_id"] for g in guides]
        titles = [g["title"] for g in guides]
        source_urls = [g.get("source_url", "") for g in guides]
        # Use strict resolution in auto-retrieval too, so source locking won't
        # drift due to fuzzy title fallback from pointer rows.
        contents = await self.get_guide_contents_strict(
            page_ids, titles=titles, source_urls=source_urls
        )
        content_by_id = {c["page_id"]: c["content"] for c in contents}
        resolved_id_by_id = {
            c["page_id"]: c.get("resolved_page_id", c["page_id"]) for c in contents
        }
        docs: list[dict[str, str]] = []
        for g in guides:
            content = content_by_id.get(g["page_id"], "")
            if content:
                resolved_page_id = resolved_id_by_id.get(g["page_id"], g["page_id"])
                docs.append(
                    {
                        "page_id": resolved_page_id,
                        "title": g["title"],
                        "content": content,
                    }
                )
        async with self._cache_lock:
            self._guide_docs_cache[db_id] = docs
        return docs

    async def get_all_guide_documents_with_images(
        self, db_id: str
    ) -> list[dict[str, str]]:
        """Like get_all_guide_documents but runs Claude Vision on image blocks.

        Intended for RAG indexing only (slow but thorough).
        """
        docs = await self.get_all_guide_documents(db_id)
        enriched: list[dict[str, str]] = []
        for doc in docs:
            page_id = doc["page_id"]
            try:
                content = await self.get_page_content(
                    page_id, extract_images=True
                )
            except Exception:
                logger.warning("Image extraction failed for %s, using cached text", page_id)
                content = doc["content"]
            enriched.append({
                "page_id": page_id,
                "title": doc["title"],
                "content": content or doc["content"],
            })
        return enriched

    async def _find_content_page_by_title(self, title: str) -> str:
        """Search for a standalone page by title and return its content."""
        resp = await self._post("/search", json={"query": title})
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

    async def clear_knowledge_cache(self) -> None:
        """Clear in-memory caches for Notion knowledge artifacts."""
        async with self._cache_lock:
            self._guide_pages_cache.clear()
            self._guide_docs_cache.clear()
            self._faq_cache.clear()

    async def add_faq_correction(
        self,
        db_id: str,
        question_text: str,
        answer_text: str,
    ) -> str:
        """Create a FAQ row in Notion from an admin-corrected answer.

        Returns the created page ID.
        """
        schema_resp = await self._get(f"/databases/{_format_uuid(db_id)}")
        schema_resp.raise_for_status()
        schema = schema_resp.json().get("properties", {})
        title_prop_name = None
        for name, prop in schema.items():
            if prop.get("type") == "title":
                title_prop_name = name
                break
        if not title_prop_name:
            raise ValueError("No title property found in FAQ database schema")

        answer_prop_name = None
        for candidate in ("answer", "תשובה"):
            prop = schema.get(candidate)
            if prop and prop.get("type") in {"rich_text", "title"}:
                answer_prop_name = candidate
                break
        if not answer_prop_name:
            for name, prop in schema.items():
                if name == title_prop_name:
                    continue
                if prop.get("type") == "rich_text":
                    answer_prop_name = name
                    break

        question_value = question_text[:1900]
        answer_value = answer_text[:1900]
        properties: dict[str, Any] = {
            title_prop_name: {
                "title": [
                    {
                        "type": "text",
                        "text": {"content": question_value},
                    }
                ]
            }
        }
        if answer_prop_name:
            if schema[answer_prop_name].get("type") == "title":
                properties[answer_prop_name] = {
                    "title": [
                        {
                            "type": "text",
                            "text": {"content": answer_value},
                        }
                    ]
                }
            else:
                properties[answer_prop_name] = {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {"content": answer_value},
                        }
                    ]
                }

        resp = await self._post(
            "/pages",
            json={
                "parent": {"database_id": _format_uuid(db_id)},
                "properties": properties,
            },
        )
        resp.raise_for_status()
        page_id = resp.json().get("id", "")
        if self._faq_cache.get(db_id) is not None:
            async with self._cache_lock:
                self._faq_cache.pop(db_id, None)
        return page_id

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> httpx.Response:
        started = time.perf_counter()
        async with self._http_semaphore:
            response = await self._client.get(path, params=params)
        logger.info(
            "notion_timing %s",
            {
                "method": "GET",
                "path": path,
                "latency_ms": round((time.perf_counter() - started) * 1000, 2),
                "status_code": response.status_code,
            },
        )
        return response

    async def _post(
        self, path: str, json: dict[str, Any] | None = None
    ) -> httpx.Response:
        started = time.perf_counter()
        async with self._http_semaphore:
            response = await self._client.post(path, json=json)
        logger.info(
            "notion_timing %s",
            {
                "method": "POST",
                "path": path,
                "latency_ms": round((time.perf_counter() - started) * 1000, 2),
                "status_code": response.status_code,
            },
        )
        return response
