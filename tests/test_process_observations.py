import pytest
from core.services import process_batch_v1
from core.errors import ReferenceDataError


@pytest.mark.asyncio
async def test_process_batch_v1_successfully(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_movebank_client_class,
    mock_pubsub_client,
    observations_batch_v1,
):
    # Mock external dependencies
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.portal", mock_gundi_client)
    mocker.patch("core.dispatchers.MovebankClient", mock_movebank_client_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    await process_batch_v1(observations_batch_v1)
    # Check that the tag data was sent o Movebank
    assert mock_movebank_client_class.called
    assert mock_movebank_client_class.return_value.post_tag_data.called


@pytest.mark.asyncio
async def test_process_batch_v1_is_retried_on_movebank_error(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_movebank_client_class_with_error_once,
    mock_pubsub_client,
    observations_batch_v1,
):
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.portal", mock_gundi_client)
    mocker.patch("core.dispatchers.MovebankClient", mock_movebank_client_class_with_error_once)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    await process_batch_v1(observations_batch_v1)
    # Check that the call to movebank was retried
    assert mock_movebank_client_class_with_error_once.called
    assert mock_movebank_client_class_with_error_once.return_value.post_tag_data.called
    total_calls = len(observations_batch_v1) + 1  # one retry
    assert mock_movebank_client_class_with_error_once.return_value.post_tag_data.call_count == total_calls


@pytest.mark.asyncio
async def test_process_batch_v1_is_retried_on_portal_error(
    mocker,
    mock_cache,
    mock_gundi_client_with_error_once,
    mock_movebank_client_class,
    mock_pubsub_client,
    observations_batch_v1,
):
    # Mock external dependencies
    mocker.patch("core.utils._cache_db", mock_cache)
    mocker.patch("core.utils.portal", mock_gundi_client_with_error_once)
    mocker.patch("core.dispatchers.MovebankClient", mock_movebank_client_class)
    mocker.patch("core.utils.pubsub", mock_pubsub_client)
    await process_batch_v1(observations_batch_v1)
    # Check that the call to the portal was retried
    assert mock_gundi_client_with_error_once.get_outbound_integration.called
    total_calls = len(observations_batch_v1) + 1  # one retry
    assert mock_gundi_client_with_error_once.get_outbound_integration.call_count == total_calls
    # Check that the tag data was sent o Movebank
    assert mock_movebank_client_class.called
    assert mock_movebank_client_class.return_value.post_tag_data.called
