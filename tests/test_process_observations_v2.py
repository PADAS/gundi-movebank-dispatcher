from unittest.mock import ANY, call
import pytest
from core import settings
from core.errors import DispatcherException
from core.services import process_batch_v2


@pytest.mark.asyncio
async def test_process_batch_v2_successfully(
    mocker,
    mock_cache,
    mock_gundi_client_v2_class,
    mock_movebank_client_class,
    mock_pubsub_client,
    dispatched_event,
    observations_batch_v2
):
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.MovebankClient", mock_movebank_client_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    await process_batch_v2(observations_batch_v2)
    assert observations_batch_v2
    # Check that the tag data was sent o Movebank
    assert mock_movebank_client_class.called
    assert mock_movebank_client_class.return_value.post_tag_data.called
    # Check that a system event was published to pubsub
    assert mock_pubsub_client.PublisherClient.called
    assert mock_pubsub_client.PubsubMessage.called
    assert mock_pubsub_client.PublisherClient.called
    assert mock_pubsub_client.PublisherClient.return_value.publish.called
