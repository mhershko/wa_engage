"""add notion_page_meta table

Revision ID: g6h7i8j9k0l1
Revises: f5g6h7i8j9k0
Create Date: 2026-03-12

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "g6h7i8j9k0l1"
down_revision: Union[str, None] = "f5g6h7i8j9k0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "notion_page_meta",
        sa.Column("notion_page_id", sa.String(length=64), primary_key=True),
        sa.Column("title", sa.String(length=500), nullable=False, server_default=""),
        sa.Column("purpose", sa.Text(), nullable=False, server_default=""),
        sa.Column("source_type", sa.String(length=32), nullable=False, server_default="other"),
        sa.Column("is_auto_generated", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_notion_page_meta_source_type", "notion_page_meta", ["source_type"])

    meta = sa.table(
        "notion_page_meta",
        sa.column("notion_page_id", sa.String),
        sa.column("title", sa.String),
        sa.column("purpose", sa.Text),
        sa.column("source_type", sa.String),
        sa.column("is_auto_generated", sa.Boolean),
    )
    op.bulk_insert(meta, [
        {
            "notion_page_id": "30e5e003-0d93-804c-9926-e8f98b5d8084",
            "title": "תאריכים מחזור 5",
            "purpose": "תאריכי כל המפגשים והאירועים במחזור 5: מפגשי מובילים, מפגש פתיחה, שולחנות עגולים, מיטאפ סיום, דדליינים ומועדים חשובים",
            "source_type": "guide",
            "is_auto_generated": False,
        },
        {
            "notion_page_id": "30e5e003-0d93-8046-b38c-d5d3b0ae2153",
            "title": "הנחיות אחרי שהרכבתם קבוצה",
            "purpose": "מה לעשות אחרי שהרכבתם קבוצה: שליחת זימוני זום, פתיחת קבוצת וואצאפ, הכנה למפגש פתיחה, מצגת פתיחה, סבב היכרות וירטואלי",
            "source_type": "guide",
            "is_auto_generated": False,
        },
        {
            "notion_page_id": "30e5e003-0d93-8036-9b02-c21525869da3",
            "title": "עריכת הרצאות",
            "purpose": "איך להגיש בקשה לעריכת הרצאה ליוטיוב: מילוי טופס בקשה, הכנת קאבר ב-Canva, תנאי עדיפות לעריכה",
            "source_type": "guide",
            "is_auto_generated": False,
        },
        {
            "notion_page_id": "30e5e003-0d93-80d2-b52b-d41a42815ef1",
            "title": "יצירת בדג׳ים",
            "purpose": "מדריך שלב-אחר-שלב ליצירת בדג׳ים (תעודות) למובילים ולמרצים באתר Certifier: סוגי בדג׳, מילוי פרטים ושליחה במייל",
            "source_type": "guide",
            "is_auto_generated": False,
        },
        {
            "notion_page_id": "30e5e003-0d93-80b7-9974-e173fdc0c662",
            "title": "העלאת הרצאות לאתר",
            "purpose": "איך מעלים הרצאות לאתר TechGym: הכנת אקסל הרצאות, ייבוא CSV דרך אדמין פאנל, הגדרת רשימות הרשאות ומזהי גוגל קלנדר",
            "source_type": "guide",
            "is_auto_generated": False,
        },
        {
            "notion_page_id": "30e5e003-0d93-80e6-baa2-de08064c64db",
            "title": "שולחנות עגולים",
            "purpose": "מה זה מפגש שולחן עגול, איך מנהלים דיון קבוצתי, תפקיד מוביל השולחן, לוז המפגש הפרונטלי בשבוע 5, דוגמאות לדילמות וטיפים",
            "source_type": "guide",
            "is_auto_generated": False,
        },
        {
            "notion_page_id": "30e5e003-0d93-803e-8dc3-f352606a3f35",
            "title": "הרכבת קבוצה",
            "purpose": "תהליך הרכבת קבוצה מלא: לוז שלבי ההרשמה והשיבוץ, קריטריונים לבחירת משתתפים, כללי גיוון, טמפלטים להודעות קבלה, אקסל ניהול",
            "source_type": "guide",
            "is_auto_generated": False,
        },
        {
            "notion_page_id": "30e5e003-0d93-8049-963e-c8572bbd6ce1",
            "title": "קישורים שימושיים",
            "purpose": "לינקים לתיקיות גוגל דרייב של TechGym: הקלטות מפגשים ותיעוד נוכחות",
            "source_type": "guide",
            "is_auto_generated": False,
        },
        {
            "notion_page_id": "30e5e003-0d93-80ff-929e-fb7db8402048",
            "title": "בניית סילבוס",
            "purpose": "איך לבנות סילבוס לקבוצה: לוז מפגשים שבועי, שיבוץ הרצאות משתתפים, עיצוב סילבוס ב-Canva, העלאה לאתר, ולמה ההרצאה הראשונה חייבת להיות של המוביל",
            "source_type": "guide",
            "is_auto_generated": False,
        },
        {
            "notion_page_id": "30e5e003-0d93-80e8-8036-c51c40f8b5d8",
            "title": "הנחיות אחרי שהמחזור מתחיל",
            "purpose": "צ׳קליסט שבועי למוביל אחרי שהמחזור התחיל: הכנה להרצאות, דריי ראן, הודעות תזכורת, ניהול מפגש, טפסי משוב מרצים, הוצאת בדג׳ים",
            "source_type": "guide",
            "is_auto_generated": False,
        },
    ])


def downgrade() -> None:
    op.drop_index("ix_notion_page_meta_source_type", table_name="notion_page_meta")
    op.drop_table("notion_page_meta")
