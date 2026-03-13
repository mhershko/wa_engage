from os import environ
from typing import Optional, Self

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    # API settings
    port: int = 5001
    host: str = "0.0.0.0"

    # Database settings
    db_uri: str

    # WhatsApp settings
    whatsapp_host: str
    whatsapp_basic_auth_password: Optional[str] = None
    whatsapp_basic_auth_user: Optional[str] = None

    # LLM settings
    anthropic_api_key: str
    model_name: str = "anthropic:claude-sonnet-4-5-20250929"

    # Notion settings
    notion_api_key: str
    notion_leaders_db_id: str
    notion_templates_db_id: str = ""
    notion_reminders_db_id: str = ""
    notion_faq_db_id: str = ""
    notion_guides_db_id: str = ""

    # Embedding settings (Voyage AI)
    voyage_api_key: str = ""

    # Admin WhatsApp group for escalations and commands
    admin_whatsapp_group_id: str = ""

    # Optional settings
    debug: bool = False
    log_level: str = "INFO"
    logfire_token: str = ""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        arbitrary_types_allowed=True,
        case_sensitive=False,
        extra="ignore",
    )

    @model_validator(mode="after")
    def apply_env(self) -> Self:
        if self.anthropic_api_key:
            environ["ANTHROPIC_API_KEY"] = self.anthropic_api_key

        if self.logfire_token:
            environ["LOGFIRE_TOKEN"] = self.logfire_token

        return self


@lru_cache
def get_settings() -> Settings:
    return Settings.model_validate({})
