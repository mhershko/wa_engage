"""Background reminder scheduler for Jimmy bot.

Uses asyncio tasks to send weekly meeting reminders and global event
reminders to leaders via DM, based on rules stored in Notion.
"""

import asyncio
import logging
import re
from datetime import datetime, timedelta

import zoneinfo

from sqlalchemy.ext.asyncio import async_sessionmaker

from config import Settings
from jimmy.notion_client import LeaderRecord, NotionClient, ReminderRule
from jimmy import templates as T
from models.reminder_log import ReminderLog
from whatsapp import SendMessageRequest, WhatsAppClient

logger = logging.getLogger(__name__)

TZ_JERUSALEM = zoneinfo.ZoneInfo("Asia/Jerusalem")

WEEKDAY_MAP: dict[str, int] = {
    "ראשון": 6,
    "שני": 0,
    "שלישי": 1,
    "רביעי": 2,
    "חמישי": 3,
    "שישי": 4,
    "שבת": 5,
    "sunday": 6,
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
}


def _parse_offset(offset: str) -> timedelta | None:
    """Parse offset strings like '1_day_before', '3_hours_before'."""
    m = re.match(r"(\d+)_(day|hour|hours|days|minute|minutes)_before", offset)
    if not m:
        return None
    amount = int(m.group(1))
    unit = m.group(2)
    if unit.startswith("day"):
        return timedelta(days=amount)
    if unit.startswith("hour"):
        return timedelta(hours=amount)
    if unit.startswith("minute"):
        return timedelta(minutes=amount)
    return None


def _next_meeting_datetime(
    weekday_name: str, time_str: str, now: datetime
) -> datetime | None:
    """Compute the next occurrence of a given weekday + time in Jerusalem TZ."""
    weekday_name_lower = weekday_name.strip().lower()
    target_weekday = WEEKDAY_MAP.get(weekday_name_lower)
    if target_weekday is None:
        return None

    parts = time_str.strip().split(":")
    if len(parts) < 2:
        return None
    try:
        hour, minute = int(parts[0]), int(parts[1])
    except ValueError:
        return None

    now_local = now.astimezone(TZ_JERUSALEM)
    days_ahead = (target_weekday - now_local.weekday()) % 7
    if days_ahead == 0:
        candidate = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if candidate <= now_local:
            days_ahead = 7
    meeting = now_local.replace(
        hour=hour, minute=minute, second=0, microsecond=0
    ) + timedelta(days=days_ahead)
    return meeting


class ReminderScheduler:
    """Periodically checks Notion reminder rules and sends DMs to leaders."""

    def __init__(
        self,
        settings: Settings,
        notion: NotionClient,
        whatsapp: WhatsAppClient,
        session_factory: async_sessionmaker,  # type: ignore[type-arg]
    ):
        self._settings = settings
        self._notion = notion
        self._wa = whatsapp
        self._session_factory = session_factory
        self._task: asyncio.Task[None] | None = None
        self._running = False

    def start(self) -> None:
        if self._task is not None:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("ReminderScheduler started")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("ReminderScheduler stopped")

    async def _loop(self) -> None:
        while self._running:
            try:
                await self._tick()
            except Exception:
                logger.exception("Error in reminder tick")
            await asyncio.sleep(300)  # check every 5 minutes

    async def _tick(self) -> None:
        now = datetime.now(TZ_JERUSALEM)

        if not self._settings.notion_reminders_db_id:
            return

        rules = await self._notion.get_reminder_rules(
            self._settings.notion_reminders_db_id
        )
        enabled_rules = [r for r in rules if r.enabled]
        if not enabled_rules:
            return

        leaders = await self._notion.get_all_leaders(
            self._settings.notion_leaders_db_id
        )

        weekly_rules = [r for r in enabled_rules if r.rule_type == "weekly_meeting"]
        global_rules = [r for r in enabled_rules if r.specific_datetime]

        for leader in leaders:
            for rule in weekly_rules:
                await self._check_weekly_reminder(leader, rule, now)

        for rule in global_rules:
            await self._check_global_reminder(leaders, rule, now)

    async def _check_weekly_reminder(
        self, leader: LeaderRecord, rule: ReminderRule, now: datetime
    ) -> None:
        if not leader.meeting_weekday or not leader.meeting_time:
            return
        if not leader.is_leader:
            return

        meeting_dt = _next_meeting_datetime(
            leader.meeting_weekday, leader.meeting_time, now
        )
        if meeting_dt is None:
            return

        if not rule.offset:
            return
        offset = _parse_offset(rule.offset)
        if offset is None:
            return

        reminder_dt = meeting_dt - offset
        if abs((now - reminder_dt).total_seconds()) > 300:
            return

        template_text = ""
        if rule.template_event_type and self._settings.notion_templates_db_id:
            try:
                tpls = await self._notion.get_templates_by_event_type(
                    rule.template_event_type, self._settings.notion_templates_db_id
                )
                if tpls:
                    template_text = tpls[0].template_text
            except Exception:
                logger.exception("Failed to fetch template for reminder")

        msg = T.WEEKLY_MEETING_REMINDER.format(
            time=leader.meeting_time,
            template_text=template_text or "(לא נמצא טמפלט)",
        )

        leader_jid = f"{leader.phone}@s.whatsapp.net"
        await self._wa.send_message(SendMessageRequest(phone=leader_jid, message=msg))
        await self._log_reminder(leader.phone, "weekly_meeting", msg)

    async def _check_global_reminder(
        self,
        leaders: list[LeaderRecord],
        rule: ReminderRule,
        now: datetime,
    ) -> None:
        if not rule.specific_datetime:
            return

        try:
            target = datetime.fromisoformat(rule.specific_datetime)
            if target.tzinfo is None:
                target = target.replace(tzinfo=TZ_JERUSALEM)
        except ValueError:
            return

        if abs((now - target).total_seconds()) > 300:
            return

        message_text = rule.message_text or ""
        if rule.template_event_type and self._settings.notion_templates_db_id:
            try:
                tpls = await self._notion.get_templates_by_event_type(
                    rule.template_event_type, self._settings.notion_templates_db_id
                )
                if tpls:
                    message_text += "\n\n" + tpls[0].template_text
            except Exception:
                logger.exception("Failed to fetch template for global reminder")

        if not message_text:
            return

        msg = T.GLOBAL_EVENT_REMINDER.format(message_text=message_text)

        for leader in leaders:
            leader_jid = f"{leader.phone}@s.whatsapp.net"
            try:
                await self._wa.send_message(
                    SendMessageRequest(phone=leader_jid, message=msg)
                )
                await self._log_reminder(leader.phone, "global_event", msg)
            except Exception:
                logger.exception("Failed to send global reminder to %s", leader.phone)

    async def _log_reminder(self, phone: str, reminder_type: str, message: str) -> None:
        async with self._session_factory() as session:
            log = ReminderLog(
                leader_phone=phone,
                reminder_type=reminder_type,
                message_sent=message[:2000],
            )
            session.add(log)
            await session.commit()
