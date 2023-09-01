import datetime
import itertools
from unittest.mock import DEFAULT
import aiohttp
import pytest
import asyncio
from gundi_core.schemas import OutboundConfiguration
from redis import exceptions as redis_exceptions
import gundi_core.schemas.v2 as schemas_v2
from gundi_core import events as system_events
from gcloud.aio import pubsub
from core import settings
from core.errors import ReferenceDataError
from movebank_client import MBClientError, MBValidationError


def async_return(result):
    f = asyncio.Future()
    f.set_result(result)
    return f


@pytest.fixture
def mock_cache(mocker):
    mock_cache = mocker.MagicMock()
    mock_cache.get.return_value = None
    return mock_cache


@pytest.fixture
def dispatched_event():
    return schemas_v2.DispatchedObservation(
        gundi_id="23ca4b15-18b6-4cf4-9da6-36dd69c6f638",
        related_to=None,
        external_id="ABC123",  # ID returned by the destination system
        data_provider_id="ddd0946d-15b0-4308-b93d-e0470b6d33b6",
        destination_id="f24281da-253b-4d92-b821-751c39e4be23",
        delivered_at=datetime.datetime.now()  # UTC
    )


@pytest.fixture
def mock_cache_with_cached_event(mocker, dispatched_event):
    mock_cache = mocker.MagicMock()
    mock_cache.get.side_effect = (None, dispatched_event.json())
    return mock_cache


@pytest.fixture
def mock_cache_with_connection_error(mocker):
    mock_cache = mocker.MagicMock()
    mock_cache.get.side_effect = redis_exceptions.ConnectionError(
        "Error while reading from 172.22.161.3:6379 : (104, 'Connection reset by peer')"
    )
    return mock_cache


@pytest.fixture
def mock_gundi_client(
        mocker,
        inbound_integration_config,
        outbound_configuration_movebank,
        device,
):
    mock_client = mocker.MagicMock()
    mock_client.get_outbound_integration.return_value = async_return(
        outbound_configuration_movebank
    )
    return mock_client

@pytest.fixture
def mock_gundi_client_with_error_once(
    mocker,
    outbound_configuration_movebank
):
    mock_client = mocker.MagicMock()
    mock_client.get_outbound_integration.return_value = async_return(
        outbound_configuration_movebank
    )
    error = ReferenceDataError(
        "Error retrieving integration details from the portal (v2): Server error '502 Bad Gateway' for url 'http://api.gundiservice.org/api/v2/integrations/f24281da-253b-4d92-b821-751c39e4be23/'. For more information check: https://httpstatuses.com/502"
    )
    mock_client.get_outbound_integration.side_effect = itertools.chain(
        [error],
        itertools.repeat(DEFAULT)
    )
    mock_client.__aenter__.return_value = mock_client
    return mock_client


@pytest.fixture
def observation_delivered_pubsub_message():
    return pubsub.PubsubMessage(
        b'{"event_id": "c05cf942-f543-4798-bd91-0e38a63d655e", "timestamp": "2023-07-12 20:34:07.210731+00:00", "schema_version": "v1", "payload": {"gundi_id": "23ca4b15-18b6-4cf4-9da6-36dd69c6f638", "related_to": "None", "external_id": "7f42ab47-fa7a-4a7e-acc6-cadcaa114646", "data_provider_id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6", "destination_id": "f24281da-253b-4d92-b821-751c39e4be23", "delivered_at": "2023-07-12 20:34:07.210542+00:00"}, "event_type": "ObservationDelivered"}'
    )


@pytest.fixture
def mock_pubsub_client(mocker, observation_delivered_pubsub_message, gcp_pubsub_publish_response):
    mock_client = mocker.MagicMock()
    mock_publisher = mocker.MagicMock()
    mock_publisher.publish.return_value = async_return(gcp_pubsub_publish_response)
    mock_publisher.topic_path.return_value = f"projects/{settings.GCP_PROJECT_ID}/topics/{settings.DISPATCHER_EVENTS_TOPIC}"
    mock_client.PublisherClient.return_value = mock_publisher
    mock_client.PubsubMessage.return_value = observation_delivered_pubsub_message
    return mock_client


@pytest.fixture
def observation_delivery_failure_pubsub_message():
    return pubsub.PubsubMessage(
        b'{"event_id": "a13c5742-8199-404b-a41b-f520d7462d74", "timestamp": "2023-07-13 14:17:13.323863+00:00", "schema_version": "v1", "payload": {"gundi_id": "9f86ae28-99c4-473f-be13-fb92bd0bc341", "related_to": null, "external_id": null, "data_provider_id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6", "destination_id": "f24281da-253b-4d92-b821-751c39e4be23", "delivered_at": "2023-07-13 14:17:13.323706+00:00"}, "event_type": "ObservationDeliveryFailed"}')


@pytest.fixture
def mock_pubsub_client_with_observation_delivery_failure(mocker, observation_delivery_failure_pubsub_message,
                                                         gcp_pubsub_publish_response):
    mock_client = mocker.MagicMock()
    mock_publisher = mocker.MagicMock()
    mock_publisher.publish.return_value = async_return(gcp_pubsub_publish_response)
    mock_publisher.topic_path.return_value = f"projects/{settings.GCP_PROJECT_ID}/topics/{settings.DISPATCHER_EVENTS_TOPIC}"
    mock_client.PublisherClient.return_value = mock_publisher
    mock_client.PubsubMessage.return_value = observation_delivery_failure_pubsub_message
    return mock_client


@pytest.fixture
def mock_movebank_response():
    # Movebank's API doesn't return any content, just 200 OK.
    return ""


@pytest.fixture
def movebank_client_close_response():
    return {}


@pytest.fixture
def mock_movebank_client_class(
        mocker,
        mock_movebank_response
):
    mocked_movebank_client_class = mocker.MagicMock()
    movebank_client_mock = mocker.MagicMock()
    movebank_client_mock.post_tag_data.return_value = async_return(
        mock_movebank_response
    )
    movebank_client_mock.__aenter__.return_value = movebank_client_mock
    movebank_client_mock.__aexit__.return_value = mock_movebank_response
    mocked_movebank_client_class.return_value = movebank_client_mock
    return mocked_movebank_client_class


@pytest.fixture
def mock_movebank_client_class_with_error_once(
        mocker,
        mock_movebank_response,
        movebank_client_close_response
):
    mocked_movebank_client_class = mocker.MagicMock()
    movebank_client_mock = mocker.MagicMock()
    movebank_client_mock.post_tag_data.return_value = async_return(
        mock_movebank_response
    )
    error = MBClientError('Movebank service unavailable')
    movebank_client_mock.post_tag_data.side_effect = itertools.chain(
        [error],
        itertools.repeat(DEFAULT)
    )
    movebank_client_mock.__aenter__.return_value = movebank_client_mock
    movebank_client_mock.__aexit__.return_value = movebank_client_close_response
    mocked_movebank_client_class.return_value = movebank_client_mock
    return mocked_movebank_client_class


@pytest.fixture
def mock_get_cloud_storage(mocker):
    return mocker.MagicMock()


@pytest.fixture
def gcp_pubsub_publish_response():
    return {"messageIds": ["7061707768812258"]}


@pytest.fixture
def inbound_integration_config():
    return {
        "state": {},
        "id": "12345b4f-88cd-49c4-a723-0ddff1f580c4",
        "type": "1234e5bd-a473-4c02-9227-27b6134615a4",
        "owner": "1234191a-bcf3-471b-9e7d-6ba8bc71be9e",
        "endpoint": "https://logins.testbidtrack.co.za/restintegration/",
        "login": "test",
        "password": "test",
        "token": "",
        "type_slug": "bidtrack",
        "provider": "bidtrack",
        "default_devicegroup": "1234cfdc-1aae-44b0-8e0a-22c72355ea85",
        "enabled": True,
        "name": "BidTrack - Manyoni",
    }


@pytest.fixture
def device():
    return {
        "id": "564daff9-8cac-4004-b1a4-e169cf3b4bdb",
        "external_id": "018910999",
        "name": "",
        "subject_type": None,
        "inbound_configuration": {
            "state": {},
            "id": "36485b4f-88cd-49c4-a723-0ddff1f580c4",
            "type": "b069e5bd-a473-4c02-9227-27b6134615a4",
            "owner": "088a191a-bcf3-471b-9e7d-6ba8bc71be9e",
            "endpoint": "https://logins.bidtrack.co.za/restintegration/",
            "login": "ZULULANDRR",
            "password": "Rh1n0#@!",
            "token": "",
            "type_slug": "bidtrack",
            "provider": "bidtrack",
            "default_devicegroup": "0da5cfdc-1aae-44b0-8e0a-22c72355ea85",
            "enabled": True,
            "name": "BidTrack - Manyoni",
        },
        "additional": {},
    }


@pytest.fixture
def unprocessed_observation_position():
    return b'{"attributes": {"observation_type": "ps"}, "data": {"id": null, "owner": "na", "integration_id": "36485b4f-88cd-49c4-a723-0ddff1f580c4", "device_id": "018910980", "name": "Logistics Truck test", "type": "tracking-device", "subject_type": null, "recorded_at": "2023-03-03 09:34:00+02:00", "location": {"x": 35.43935, "y": -1.59083, "z": 0.0, "hdop": null, "vdop": null}, "additional": {"voltage": "7.4", "fuel_level": 71, "speed": "41 kph"}, "voltage": null, "temperature": null, "radio_status": null, "observation_type": "ps"}}'


@pytest.fixture
def transformed_observation_position():
    return {
        "manufacturer_id": "018910980",
        "source_type": "tracking-device",
        "subject_name": "Logistics Truck test",
        "recorded_at": datetime.datetime(
            2023,
            3,
            2,
            18,
            47,
            tzinfo=datetime.timezone(datetime.timedelta(seconds=7200)),
        ),
        "location": {"lon": 35.43929, "lat": -1.59083},
        "additional": {"voltage": "7.4", "fuel_level": 71, "speed": "41 kph"},
    }


@pytest.fixture
def transformed_observation_attributes():
    return {
        "observation_type": "ps",
        "device_id": "018910980",
        "outbound_config_id": "1c19dc7e-73e2-4af3-93f5-a1cb322e5add",
        "integration_id": "36485b4f-88cd-49c4-a723-0ddff1f580c4",
    }


@pytest.fixture
def transformed_observation_gcp_message():
    return b'{"manufacturer_id": "018910980", "source_type": "tracking-device", "subject_name": "Logistics Truck test", "recorded_at": "2023-03-02 18:47:00+02:00", "location": {"lon": 35.43929, "lat": -1.59083}, "additional": {"voltage": "7.4", "fuel_level": 71, "speed": "41 kph"}}'


@pytest.fixture
def outbound_configuration_movebank():
    return {
        'id': 'd16ed1e1-a5e8-4942-b264-bbf5316dda55', 'type': 'bc8c091c-ca5b-4018-b649-2650adcf86a5',
        'owner': 'e2d1b0fc-69fe-408b-afc5-7f54872730c0', 'name': 'Movebank test',
        'endpoint': 'https://www.movebank.mpg.de',
        'state': {},
        'login': 'adminuser', 'password': 'S@m3Passw0rD',
        'token': '', 'type_slug': 'movebank',
        'additional': {
            'feed': 'gundi/earthranger',
            'topic': 'destination-movebank-dev',
            'broker': 'gcp_pubsub'
        }
    }


@pytest.fixture
def mock_gundi_client_v2(
        mocker,
        destination_integration_v2,
):
    mock_client = mocker.MagicMock()
    mock_client.get_integration_details.return_value = async_return(
        destination_integration_v2
    )
    mock_client.__aenter__.return_value = mock_client
    return mock_client


@pytest.fixture
def mock_gundi_client_v2_with_error_once(
        mocker,
        destination_integration_v2,
):
    mock_client = mocker.MagicMock()
    mock_client.get_integration_details.return_value = async_return(
        destination_integration_v2
    )
    error = ReferenceDataError(
        "Error retrieving integration details from the portal (v2): Server error '502 Bad Gateway' for url 'http://api.gundiservice.org/api/v2/integrations/f24281da-253b-4d92-b821-751c39e4be23/'. For more information check: https://httpstatuses.com/502"
    )
    mock_client.get_integration_details.side_effect = itertools.chain(
        [error],
        itertools.repeat(DEFAULT)
    )
    mock_client.__aenter__.return_value = mock_client
    return mock_client


@pytest.fixture
def mock_gundi_client_v2_class_with_error_once(mocker, mock_gundi_client_v2_with_error_once):
    mock_gundi_client_v2_class = mocker.MagicMock()
    mock_gundi_client_v2_class.return_value = mock_gundi_client_v2_with_error_once
    return mock_gundi_client_v2_class


@pytest.fixture
def mock_gundi_client_v2_class(mocker, mock_gundi_client_v2):
    mock_gundi_client_v2_class = mocker.MagicMock()
    mock_gundi_client_v2_class.return_value = mock_gundi_client_v2
    return mock_gundi_client_v2_class


@pytest.fixture
def destination_integration_v2():
    return schemas_v2.Integration.parse_obj(
        {
            'id': 'f24281da-253b-4d92-b821-751c39e4be23',
            'name': 'Movebank Destination',
            'base_url': 'https://www.movebank.mpg.de',
            'enabled': True,
            'type': {
                'id': '8aa2e415-8bf2-4162-b870-d85a66de48e1',
                'name': 'Move Bank',
                'value': 'move_bank',
                'description': 'Integration type for Move Bank platform',
                'actions': [
                    {'id': '39e51ddc-8768-4a3d-9e80-c4b77ebbb10a', 'type': 'auth', 'name': 'Authenticate',
                     'value': 'auth', 'description': '',
                     'schema': {'type': 'object', 'required': ['username', 'password'],
                                'properties': {'password': {'type': 'string'}, 'username': {'type': 'string'}}}},
                    {'id': '613a09db-4334-4cdf-914c-992cc0bd8ee7', 'type': 'auth', 'name': 'Permissions',
                     'value': 'permissions', 'description': 'Set permissions per tag_id and user',
                     'schema': {'type': 'object', 'title': 'MBPermissionsActionConfig', 'properties': {
                         'study': {'type': 'string', 'title': 'Study', 'default': 'gundi', 'example': 'gundi',
                                   'description': 'Name of the movebank study'},
                         'permissions': {'type': 'array', 'items': {'$ref': '#/definitions/MBUserPermission'},
                                         'title': 'Permissions'}}, 'definitions': {
                         'MBUserPermission': {'type': 'object', 'title': 'MBUserPermission',
                                              'required': ['username', 'tag_id'], 'properties': {
                                 'tag_id': {'type': 'string', 'title': 'Tag Id',
                                            'example': 'awt.1320894.cc53b809784e406db9cfd8dcbc624985',
                                            'description': 'Tag ID, to grant the user access to its data.'},
                                 'username': {'type': 'string', 'title': 'Username', 'example': 'movebankuser',
                                              'description': 'Username used to login in Movebank'}}}}}},
                    {'id': 'cc427f07-4dba-49e6-a55b-264034137780', 'type': 'pull', 'name': 'Pull Observations',
                     'value': 'pull_observations', 'description': 'Pull position data from Move Bank',
                     'schema': {'type': 'object', 'required': ['endpoint'],
                                'properties': {'endpoint': {'type': 'string'}}}},
                    {'id': '1469c6bc-cdb3-4299-a0ff-91adb19f23d5', 'type': 'push', 'name': 'Push Observations',
                     'value': 'push_observations', 'description': 'Push position data to Move Bank',
                     'schema': {'type': 'object', 'required': ['feed'],
                                'properties': {'endpoint': {'type': 'gundi/earthranger'}}}}]},
            'owner': {'id': 'e2d1b0fc-69fe-408b-afc5-7f54872730c0', 'name': 'Test Organization', 'description': ''},
            'configurations': [
                {'id': '00877649-ce4a-497d-ac47-43803f42a2a6', 'integration': 'f24281da-253b-4d92-b821-751c39e4be23',
                 'action': {'id': '613a09db-4334-4cdf-914c-992cc0bd8ee7', 'type': 'auth', 'name': 'Permissions',
                            'value': 'permissions'}, 'data': {'study': 'gundi', 'permissions': [
                    {'tag_id': 'awt.test-device-orfxingdskmp.2b029799-a5a1-4794-a1bd-ac12b85f9249',
                     'username': 'marianobrc'},
                    {'tag_id': 'awt.test-device-ptyjhlnkfqgb.2b029799-a5a1-4794-a1bd-ac12b85f9249',
                     'username': 'marianobrc'},
                    {'tag_id': 'awt.test-device-qjlvtwzrynfm.2b029799-a5a1-4794-a1bd-ac12b85f9249',
                     'username': 'marianobrc'}]}},
                {'id': '30066f0e-ff33-48aa-bd00-84620a8ba8cc', 'integration': 'f24281da-253b-4d92-b821-751c39e4be23',
                 'action': {'id': '1469c6bc-cdb3-4299-a0ff-91adb19f23d5', 'type': 'push', 'name': 'Push Observations',
                            'value': 'push_observations'}, 'data': {'feed': 'gundi/earthranger'}},
                {'id': '50f404f1-8877-45d7-b2c5-5f21db42d575', 'integration': 'f24281da-253b-4d92-b821-751c39e4be23',
                 'action': {'id': '39e51ddc-8768-4a3d-9e80-c4b77ebbb10a', 'type': 'auth', 'name': 'Authenticate',
                            'value': 'auth'}, 'data': {'password': 'SomeP4ssw0rd', 'username': 'adminuser'}}],
            'additional': {}, 'default_route': None,
            'status': {'id': 'mockid-b16a-4dbd-ad32-197c58aeef59', 'is_healthy': True,
                       'details': 'Last observation has been delivered with success.',
                       'observation_delivered_24hrs': 50231,
                       'last_observation_delivered_at': '2023-03-31T11:20:00+0200'}
        }
    )


@pytest.fixture
def dispatched_observation():
    return schemas_v2.DispatchedObservation(
        gundi_id="23ca4b15-18b6-4cf4-9da6-36dd69c6f638",
        related_to=None,
        external_id="37314d00-731f-427c-aaa5-336daf13f904",  # ID returned by the destination system
        data_provider_id="ddd0946d-15b0-4308-b93d-e0470b6d33b6",
        destination_id="f24281da-253b-4d92-b821-751c39e4be23",
        delivered_at=datetime.datetime.now(datetime.timezone.utc)
    )


@pytest.fixture
def observation_delivered_event(dispatched_observation):
    return system_events.ObservationDelivered(
        payload=dispatched_observation
    )


@pytest.fixture
def observations_batch_v1():
    return [
        {
            'data': {
                'recorded_at': '2023-08-23T00:00:02Z',
                'tag_id': 'awt.test-device-ptyjhlnkfqgb.2b029799-a5a1-4794-a1bd-ac12b85f9249',
                'lon': 270.23832401973146, 'lat': -3.4126258080695777, 'sensor_type': 'GPS',
                'tag_manufacturer_name': 'AWT',
                'gundi_urn': 'urn:gundi:v1.intsrc.2b029799-a5a1-4794-a1bd-ac12b85f9249.test-device-ptyjhlnkfqgb'
            },
            'attributes': {
                'outbound_config_id': 'd16ed1e1-a5e8-4942-b264-bbf5316dda55',
                'tracing_context': '{"x-cloud-trace-context": "8fb310d02034fa671ea09f7655c2627b/8607927463316716170;o=1"}',
                'integration_id': '2b029799-a5a1-4794-a1bd-ac12b85f9249', 'observation_type': 'ps',
                'device_id': 'test-device-ptyjhlnkfqgb'
            }
        },
        {
            'data': {
                'recorded_at': '2023-08-23T00:00:01Z',
                'tag_id': 'awt.test-device-yhpoqurfnkwv.2b029799-a5a1-4794-a1bd-ac12b85f9249',
                'lon': 335.56862712178133,
                'lat': -42.089873109164685,
                'sensor_type': 'GPS',
                'tag_manufacturer_name': 'AWT',
                'gundi_urn': 'urn:gundi:v1.intsrc.2b029799-a5a1-4794-a1bd-ac12b85f9249.test-device-yhpoqurfnkwv'
            },
            'attributes': {
                'observation_type': 'ps',
                'tracing_context': '{"x-cloud-trace-context": "722e20908562f0f7d3c9e29dd427b2a4/1933959524448024112;o=1"}',
                'device_id': 'test-device-yhpoqurfnkwv',
                'outbound_config_id': 'd16ed1e1-a5e8-4942-b264-bbf5316dda55',
                'integration_id': '2b029799-a5a1-4794-a1bd-ac12b85f9249'
            }
        }
    ]


@pytest.fixture
def observations_batch_v2():
    return [
        {
            "data": {
                'recorded_at': '2023-08-18T21:38:00Z',
                'tag_id': 'awt.test-device-orfxingdskmp.2b029799-a5a1-4794-a1bd-ac12b85f9249',
                'lon': 35.43902,
                'lat': -1.59083,
                'sensor_type': 'GPS',
                'tag_manufacturer_name': 'AWT',
                'gundi_urn': 'urn:gundi:v2.intsrc.2b029799-a5a1-4794-a1bd-ac12b85f9249.test-device-orfxingdskmp'
            },
            "attributes": {
                'gundi_version': 'v2',
                'provider_key': 'ddd0946d-15b0-4308-b93d-e0470b6d33b6',
                'gundi_id': '3a2c465c-9f53-4d36-a1ac-1bb6e3c4f425',
                'related_to': None,
                'stream_type': schemas_v2.StreamPrefixEnum.observation.value,
                'source_id': 'e301408c-abe7-4658-8707-8ab0583f2b22',
                'external_source_id': 'test-device-orfxingdskmp',
                'destination_id': 'f24281da-253b-4d92-b821-751c39e4be23',
                'data_provider_id': 'ddd0946d-15b0-4308-b93d-e0470b6d33b6',
                'annotations': '{}'
            }
        },
        {
            "data": {
                'recorded_at': '2023-08-18T21:38:00Z',
                'tag_id': 'awt.test-device-yhpoqurfnkwv.2b029799-a5a1-4794-a1bd-ac12b85f9249',
                'lon': 35.43902,
                'lat': -1.59083,
                'sensor_type': 'GPS',
                'tag_manufacturer_name': 'AWT',
                'gundi_urn': 'urn:gundi:v1.intsrc.2b029799-a5a1-4794-a1bd-ac12b85f9249.test-device-yhpoqurfnkwv'
            },
            "attributes": {
                'gundi_version': 'v2',
                'provider_key': 'ddd0946d-15b0-4308-b93d-e0470b6d33b6',
                'gundi_id': '3a2c465c-9f53-4d36-a1ac-1bb6e3c4f425',
                'related_to': None,
                'stream_type': schemas_v2.StreamPrefixEnum.observation.value,
                'source_id': 'e301408c-abe7-4658-8707-8ab0583f2b22',
                'external_source_id': 'test-device-yhpoqurfnkwv',
                'destination_id': 'f24281da-253b-4d92-b821-751c39e4be23',
                'data_provider_id': 'ddd0946d-15b0-4308-b93d-e0470b6d33b6',
                'annotations': '{}'
            }
        }
    ]
