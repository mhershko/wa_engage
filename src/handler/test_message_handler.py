from unittest.mock import AsyncMock, Mock

import pytest

from whatsapp.jid import JID
from config import Settings


@pytest.fixture
def mock_whatsapp():
    client = AsyncMock()
    client.send_message = AsyncMock()
    client.get_my_jid = AsyncMock(return_value=JID(user="bot", server="s.whatsapp.net"))
    return client


@pytest.fixture
def mock_settings():
    return Mock(spec=Settings, model_name="test-model")
