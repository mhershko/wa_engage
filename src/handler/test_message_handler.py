from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock

import pytest

from handler import MessageHandler
from models import Message, WhatsAppWebhookPayload
from test_utils.mock_session import AsyncSessionMock
from whatsapp import SendMessageRequest
from whatsapp.jid import JID
from config import Settings


@pytest.fixture
def mock_whatsapp():
    client = AsyncMock()
    client.send_message = AsyncMock()
    client.get_my_jid = AsyncMock(return_value=JID(user="bot", server="s.whatsapp.net"))
    return client


@pytest.fixture
def mock_embedding_client():
    client = AsyncMock()
    return client


@pytest.fixture
def mock_settings():
    return Mock(spec=Settings, model_name="test-model", dm_autoreply_enabled=False)


