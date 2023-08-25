import pytest
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
    # Check that the tag data was sent o Movebank
    assert mock_movebank_client_class.called
    assert mock_movebank_client_class.return_value.post_tag_data.called
    # Check that a system event was published to pubsub
    assert mock_pubsub_client.PublisherClient.called
    assert mock_pubsub_client.PubsubMessage.called
    assert mock_pubsub_client.PublisherClient.called
    assert mock_pubsub_client.PublisherClient.return_value.publish.called


@pytest.mark.asyncio
async def test_process_batch_v2_is_retried_on_movebank_error(
    mocker,
    mock_cache,
    mock_movebank_client_class_with_error_once,
    mock_gundi_client_v2_class,
    mock_pubsub_client,
    observations_batch_v2
):
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("core.dispatchers.MovebankClient", mock_movebank_client_class_with_error_once)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    await process_batch_v2(observations_batch_v2)
    # Check that the call to movebank was retried
    assert mock_movebank_client_class_with_error_once.called
    assert mock_movebank_client_class_with_error_once.return_value.post_tag_data.called
    total_calls = len(observations_batch_v2) + 1  # one retry
    assert mock_movebank_client_class_with_error_once.return_value.post_tag_data.call_count == total_calls


@pytest.mark.asyncio
async def test_process_batch_v2_is_retried_on_portal_error(
    mocker,
    mock_cache,
    mock_movebank_client_class,
    mock_gundi_client_v2_class_with_error_once,
    mock_pubsub_client,
    observations_batch_v2
):
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.GundiClient", mock_gundi_client_v2_class_with_error_once)
    mocker.patch("core.dispatchers.MovebankClient", mock_movebank_client_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    await process_batch_v2(observations_batch_v2)
    # Check that the call to the portal was retried
    assert mock_gundi_client_v2_class_with_error_once.called
    assert mock_gundi_client_v2_class_with_error_once.return_value.get_integration_details.called
    total_calls = len(observations_batch_v2) + 1  # one retry
    assert mock_gundi_client_v2_class_with_error_once.return_value.get_integration_details.call_count == total_calls
    # Check that a calll to movebank was made
    assert mock_movebank_client_class.called
    assert mock_movebank_client_class.return_value.post_tag_data.called

