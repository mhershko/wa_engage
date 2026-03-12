"""Hebrew response templates for Jimmy bot.

All user-facing messages are in Hebrew.
Jimmy speaks about himself in masculine form.
Templates use gendered forms for addressing the leader based on their gender.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from jimmy.notion_client import LeaderRecord


def _g(leader: LeaderRecord) -> dict[str, str]:
    """Return a dict of gendered Hebrew word-forms for addressing the leader."""
    if leader.is_masculine:
        return {
            "at": "אתה",
            "mofiya": "מופיע",
            "movil": "מוביל",
            "pne": "פנה",
            "tirtze": "תרצה",
            "shoel": "שואל",
            "elekha": "אליך",
            "ktov": "כתוב",
            "ratzita": "רצית",
        }
    return {
        "at": "את",
        "mofiya": "מופיעה",
        "movil": "מובילה",
        "pne": "פני",
        "tirtze": "תרצי",
        "shoel": "שואלת",
        "elekha": "אלייך",
        "ktov": "כתבי",
        "ratzita": "רצית",
    }


# ---------------------------------------------------------------------------
# Onboarding
# ---------------------------------------------------------------------------

def unknown_user(leader: LeaderRecord | None = None) -> str:
    if leader and leader.is_masculine:
        return (
            "כרגע אתה לא מופיע כמוביל קבוצה במחזור הנוכחי של TechGym.\n"
            "אם זו טעות – אנא פנה לצוות TechGym."
        )
    return (
        "כרגע את לא מופיעה כמובילת קבוצה במחזור הנוכחי של TechGym.\n"
        "אם זו טעות – אנא פני לצוות TechGym."
    )

UNKNOWN_USER = unknown_user()


def welcome_message(leader: LeaderRecord) -> str:
    g = _g(leader)
    parts: list[str] = []

    if leader.has_group and leader.is_management:
        parts.append(
            f"היי {leader.name} 💚\n"
            f"זיהיתי ש{g['at']} {g['movil']} קבוצת {leader.group_name} "
            f"וגם חלק מצוות הניהול של TechGym."
        )
        if leader.meeting_weekday or leader.meeting_time:
            parts.append(
                f"המפגש השבועי שלך הוא ביום {leader.meeting_weekday or '—'} "
                f"בשעה {leader.meeting_time or '—'}."
            )
    elif leader.has_group:
        parts.append(
            f"היי {leader.name} 💚\n"
            f"זיהיתי ש{g['at']} {g['movil']} קבוצת {leader.group_name}."
        )
        if leader.meeting_weekday or leader.meeting_time:
            parts.append(
                f"המפגש השבועי שלך הוא ביום {leader.meeting_weekday or '—'} "
                f"בשעה {leader.meeting_time or '—'}."
            )
    else:
        parts.append(
            f"היי {leader.name} 💚\n"
            f"זיהיתי ש{g['at']} חלק מצוות הניהול של TechGym."
        )

    parts.append(
        "\nאני כאן כדי לעזור בשאלות אדמיניסטרטיביות, תזכורות וטמפלטים."
    )
    parts.append(
        "\nשקיפות חשובה: השאלות שלך והתשובות שלי גלויות לצוות האדמין."
    )

    if leader.has_group:
        parts.append(
            f"\nאם {g['tirtze']} שאדע גם מה קורה בקבוצת המשתתפים,\n"
            f"אפשר להוסיף אותי לקבוצת הווטסאפ — אבל זו לא חובה.\n"
            f"\nחשוב: אני *אף פעם* לא כותב בקבוצה.\n"
            f"כל השיחה איתי היא כאן, בפרטי."
        )

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Group binding
# ---------------------------------------------------------------------------

GROUP_BINDING_REQUEST = (
    "היי, הוסיפו אותי לקבוצה חדשה:\n"
    "\n"
    "שם קבוצה: {whatsapp_group_name}\n"
    "טלפון: {leader_phone}\n"
    "\n"
    "אם זו קבוצת המשתתפים של TechGym,\n"
    "אנא הגיבו בקבוצה: /approve {leader_phone}"
)

GROUP_APPROVED = (
    "אישרו לי את הקבוצה שלך.\n"
    "מעכשיו אני יכול לעקוב מאחורי הקלעים אחרי מה שנשלח "
    "(בלי לכתוב שם)."
)


def leader_reset(leader: LeaderRecord | None = None) -> str:
    g = _g(leader) if leader else {"ktov": "כתבי"}
    return f'החיבור שלך אופס.\nכדי להתחיל מחדש – פשוט {g["ktov"]} לי "היי".'


LEADER_RESET = leader_reset()

# ---------------------------------------------------------------------------
# Intent responses
# ---------------------------------------------------------------------------


def facilitation_refusal(leader: LeaderRecord) -> str:
    return (
        "זו שאלה מעולה על ההנחיה והדינמיקה של הקבוצה 🧠\n"
        "אני מתעסק רק בצד האדמיניסטרטיבי – לו״ז, שלבים בתוכנית,\n"
        "תזכורות וטמפלטים.\n"
        "\n"
        "ממליץ להעלות את זה בקבוצת המובילים\n"
        "ולקבל עצות מהמובילים האחרים ❤️"
    )


def low_confidence_escalation(leader: LeaderRecord) -> str:
    g = _g(leader)
    return (
        "אני לא בטוח שיש לי תשובה מדויקת לזה מתוך החומר שקיים.\n"
        f"אעביר את השאלה לצוות הניהול ונחזור {g['elekha']} עם תשובה מסודרת."
    )


def unknown_intent_clarification(leader: LeaderRecord) -> str:
    g = _g(leader)
    return (
        f"אני לא בטוח שהבנתי – {g['at']} {g['shoel']} על לו״ז / שלבים בתוכנית,\n"
        "או יותר על הדינמיקה של הקבוצה?"
    )


def long_processing_notice(
    leader: LeaderRecord, leader_gender: str | None = None
) -> str:
    if leader_gender == "masculine":
        g = {
            "at": "אתה",
        }
    elif leader_gender == "feminine":
        g = {
            "at": "את",
        }
    else:
        g = _g(leader)
    return (
        f"רק מעדכן ש{g['at']} שואל שאלה טובה 😊\n"
        "אני בודק את החומרים כדי להביא תשובה מדויקת,\n"
        "זה יכול לקחת עוד כמה שניות."
    )


TEMPLATE_RESPONSE = "הנה טיוטה שאפשר לשלוח בקבוצה:\n\n{template_text}"

# ---------------------------------------------------------------------------
# Escalation messages (sent to admin group)
# ---------------------------------------------------------------------------

ADMIN_ESCALATION_LOGISTICS = (
    "שאלה חדשה למענה:\n"
    "\n"
    "{movil}: {leader_name}, קבוצת {leader_group}\n"
    "טלפון: {leader_phone}\n"
    "\n"
    "שאלה:\n"
    '"{original_question}"\n'
    "\n"
    "(סווג כ־{intent_type}, אין תשובה ב-Notion)"
)

ADMIN_ESCALATION_FACILITATION = (
    "שאלה מחוץ לתחום האדמיניסטרציה:\n"
    "\n"
    "{movil}: {leader_name}, קבוצת {leader_group}\n"
    "טלפון: {leader_phone}\n"
    "\n"
    "שאלה:\n"
    '"{original_question}"\n'
    "\n"
    "(סווג כ־FACILITATION)"
)

# ---------------------------------------------------------------------------
# Reminders
# ---------------------------------------------------------------------------

WEEKLY_MEETING_REMINDER = (
    "היי 💚\n"
    "היום יש לך מפגש TechGym בשעה {time}.\n"
    "זה זמן טוב לשלוח תזכורת בקבוצה.\n"
    "\n"
    "הנה טיוטה שאפשר להעתיק:\n"
    "{template_text}"
)

GLOBAL_EVENT_REMINDER = "היי 💚\n{message_text}"
