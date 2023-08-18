import datetime
import aiohttp
import pytest
import asyncio
from gundi_core.schemas import OutboundConfiguration
from redis import exceptions as redis_exceptions
import gundi_core.schemas.v2 as schemas_v2
from gundi_core import events as system_events
from gcloud.aio import pubsub
from core import settings
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
        outbound_integration_config,
        outbound_integration_config_list,
        device,
):
    mock_client = mocker.MagicMock()
    mock_client.get_inbound_integration.return_value = async_return(
        inbound_integration_config
    )
    mock_client.get_outbound_integration.return_value = async_return(
        outbound_integration_config
    )
    mock_client.get_outbound_integration_list.return_value = async_return(
        outbound_integration_config_list
    )
    mock_client.ensure_device.return_value = async_return(device)
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
def mock_movebank_client_class_with_service_unavailable_error(
        mocker,
        mock_movebank_response,
        er_client_close_response
):
    mocked_movebank_client_class = mocker.MagicMock()
    movebank_client_mock = mocker.MagicMock()
    error = MBClientError('Movebank service unavailable')
    movebank_client_mock.post_sensor_observation.side_effect = error
    movebank_client_mock.post_report.side_effect = error
    movebank_client_mock.post_report_attachment.side_effect = error
    movebank_client_mock.post_camera_trap_report.side_effect = error
    movebank_client_mock.close.side_effect = error
    movebank_client_mock.__aenter__.return_value = movebank_client_mock
    movebank_client_mock.__aexit__.return_value = er_client_close_response
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
def outbound_integration_config():
    return {
        "id": "2222dc7e-73e2-4af3-93f5-a1cb322e5add",
        "type": "f61b0c60-c863-44d7-adc6-d9b49b389e69",
        "owner": "1111191a-bcf3-471b-9e7d-6ba8bc71be9e",
        "name": "[Internal] AI2 Test -  Bidtrack to  ER",
        "endpoint": "https://cdip-er.pamdas.org/api/v1.0",
        "state": {},
        "login": "",
        "password": "",
        "token": "1111d87681cd1d01ad07c2d0f57d15d6079ae7d7",
        "type_slug": "earth_ranger",
        "inbound_type_slug": "bidtrack",
        "additional": {},
    }


@pytest.fixture
def outbound_integration_config_list():
    return [
        {
            "id": "2222dc7e-73e2-4af3-93f5-a1cb322e5add",
            "type": "f61b0c60-c863-44d7-adc6-d9b49b389e69",
            "owner": "1111191a-bcf3-471b-9e7d-6ba8bc71be9e",
            "name": "[Internal] AI2 Test -  Bidtrack to  ER",
            "endpoint": "https://cdip-er.pamdas.org/api/v1.0",
            "state": {},
            "login": "",
            "password": "",
            "token": "1111d87681cd1d01ad07c2d0f57d15d6079ae7d7",
            "type_slug": "earth_ranger",
            "inbound_type_slug": "bidtrack",
            "additional": {},
        }
    ]


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
def outbound_configuration_gcp_pubsub():
    return OutboundConfiguration.parse_obj(
        {
            "id": "1c19dc7e-73e2-4af3-93f5-a1cb322e5add",
            "type": "f61b0c60-c863-44d7-adc6-d9b49b389e69",
            "owner": "088a191a-bcf3-471b-9e7d-6ba8bc71be9e",
            "name": "[Internal] AI2 Test -  Bidtrack to  ER load test",
            "endpoint": "https://gundi-load-testing.pamdas.org/api/v1.0",
            "state": {},
            "login": "",
            "password": "",
            "token": "0890d87681cd1d01ad07c2d0f57d15d6079ae7d7",
            "type_slug": "earth_ranger",
            "inbound_type_slug": "bidtrack",
            "additional": {"broker": "gcp_pubsub"},
        }
    )


#
# @pytest.fixture
# def position_as_cloud_event():
#     return CloudEvent(
#         attributes={
#             'specversion': '1.0',
#             'id': '123451234512345',
#             'source': '//pubsub.googleapis.com/projects/MY-PROJECT/topics/MY-TOPIC',
#             'type': 'google.cloud.pubsub.topic.v1.messagePublished', 'datacontenttype': 'application/json',
#             'time': datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
#         },
#         data={
#             "message": {
#                 "data": "eyJtYW51ZmFjdHVyZXJfaWQiOiAiMDE4OTEwOTgwIiwgInNvdXJjZV90eXBlIjogInRyYWNraW5nLWRldmljZSIsICJzdWJqZWN0X25hbWUiOiAiTG9naXN0aWNzIFRydWNrIEEiLCAicmVjb3JkZWRfYXQiOiAiMjAyMy0wMy0wNyAwODo1OTowMC0wMzowMCIsICJsb2NhdGlvbiI6IHsibG9uIjogMzUuNDM5MTIsICJsYXQiOiAtMS41OTA4M30sICJhZGRpdGlvbmFsIjogeyJ2b2x0YWdlIjogIjcuNCIsICJmdWVsX2xldmVsIjogNzEsICJzcGVlZCI6ICI0MSBrcGgifX0=",
#                 "attributes": {
#                     "observation_type": "ps",
#                     "device_id": "018910980",
#                     "outbound_config_id": "1c19dc7e-73e2-4af3-93f5-a1cb322e5add",
#                     "integration_id": "36485b4f-88cd-49c4-a723-0ddff1f580c4",
#                     "tracing_context": "{}"
#                 }
#             },
#             "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB"
#         }
#     )


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
def mock_gundi_client_v2_class(mocker, mock_gundi_client_v2):
    mock_gundi_client_v2_class = mocker.MagicMock()
    mock_gundi_client_v2_class.return_value = mock_gundi_client_v2
    return mock_gundi_client_v2_class


@pytest.fixture
def destination_integration_v2():
    return schemas_v2.Integration.parse_obj(
        {'id': 'f24281da-253b-4d92-b821-751c39e4be23', 'name': 'ER Load Testing',
         'base_url': 'https://gundi-load-testing.pamdas.org', 'enabled': True,
         'type': {'id': '45c66a61-71e4-4664-a7f2-30d465f87aa6', 'name': 'EarthRanger', 'value': 'earth_ranger',
                  'description': 'Integration type for Earth Ranger Sites', 'actions': [
                 {'id': '43ec4163-2f40-43fc-af62-bca1db77c06b', 'type': 'auth', 'name': 'Authenticate', 'value': 'auth',
                  'description': 'Authenticate against Earth Ranger',
                  'schema': {'type': 'object', 'required': ['token'], 'properties': {'token': {'type': 'string'}}}},
                 {'id': '036c2098-f494-40ec-a595-710b314d5ea5', 'type': 'pull', 'name': 'Pull Positions',
                  'value': 'pull_positions', 'description': 'Pull position data from an Earth Ranger site',
                  'schema': {'type': 'object', 'required': ['endpoint'],
                             'properties': {'endpoint': {'type': 'string'}}}},
                 {'id': '9286bb71-9aca-425a-881f-7fe0b2dba4f4', 'type': 'push', 'name': 'Push Events',
                  'value': 'push_events', 'description': 'EarthRanger sites support sending Events (a.k.a Reports)',
                  'schema': {}},
                 {'id': 'aae0cf50-fbc7-4810-84fd-53fb75020a43', 'type': 'push', 'name': 'Push Positions',
                  'value': 'push_positions', 'description': 'Push position data to an Earth Ranger site',
                  'schema': {'type': 'object', 'required': ['endpoint'],
                             'properties': {'endpoint': {'type': 'string'}}}}]},
         'owner': {'id': 'e2d1b0fc-69fe-408b-afc5-7f54872730c0', 'name': 'Test Organization', 'description': ''},
         'configurations': [
             {'id': '013ea7ce-4944-4f7e-8a2f-e5338b3741ce', 'integration': 'f24281da-253b-4d92-b821-751c39e4be23',
              'action': {'id': '43ec4163-2f40-43fc-af62-bca1db77c06b', 'type': 'auth', 'name': 'Authenticate',
                         'value': 'auth'}, 'data': {'token': '1190d87681cd1d01ad07c2d0f57d15d6079ae7ab'}},
             {'id': '5de91c7b-f28a-4ce7-8137-273ac10674d2', 'integration': 'f24281da-253b-4d92-b821-751c39e4be23',
              'action': {'id': 'aae0cf50-fbc7-4810-84fd-53fb75020a43', 'type': 'push', 'name': 'Push Positions',
                         'value': 'push_positions'}, 'data': {'endpoint': 'api/v1/positions'}},
             {'id': '7947b19e-1d2d-4ca3-bd6c-74976ae1de68', 'integration': 'f24281da-253b-4d92-b821-751c39e4be23',
              'action': {'id': '036c2098-f494-40ec-a595-710b314d5ea5', 'type': 'pull', 'name': 'Pull Positions',
                         'value': 'pull_positions'}, 'data': {'endpoint': 'api/v1/positions'}}],
         'additional': {'topic': 'destination-v2-f24281da-253b-4d92-b821-751c39e4be23-dev', 'broker': 'gcp_pubsub'},
         'default_route': {'id': '38dd8ec2-b3ee-4c31-940e-b6cc9c1f4326', 'name': 'Mukutan - Load Testing'},
         'status': {'id': 'mockid-b16a-4dbd-ad32-197c58aeef59', 'is_healthy': True,
                    'details': 'Last observation has been delivered with success.',
                    'observation_delivered_24hrs': 50231, 'last_observation_delivered_at': '2023-03-31T11:20:00+0200'}
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
                'recorded_at': '2023-08-18T00:00:02Z',
                'tag_id': 'awt.test-device-yhpoqurfnkwv.2b029799-a5a1-4794-a1bd-ac12b85f9249',
                'lon': -73.23378944234764,
                'lat': -46.157904554790974,
                'sensor_type': 'GPS',
                'tag_manufacturer_name': 'YhfcRihikbZjTnRbhmfo',
                'gundi_urn': 'urn:gundi:v1.intsrc.2b029799-a5a1-4794-a1bd-ac12b85f9249.test-device-yhpoqurfnkwv'
            },
            'attributes': {
                'device_id': 'test-device-yhpoqurfnkwv',
                'observation_type': 'ps',
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
        }
    ]
