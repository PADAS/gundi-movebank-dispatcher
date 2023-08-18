import asyncio
import json
import logging
import aiohttp
from datetime import datetime, timezone
from gcloud.aio import pubsub
from itertools import groupby
from opentelemetry.trace import SpanKind
from core import dispatchers
from core.utils import (
    get_outbound_config_detail,
    ExtraKeys,
    get_integration_details,
    get_dispatched_observation,
    cache_dispatched_observation,
    is_null,
    publish_event,
)
from gundi_core.schemas import v2 as gundi_schemas_v2
from gundi_core import events as system_events
from .errors import DispatcherException, ReferenceDataError
from . import settings
from . import tracing
from .dispatchers import MBTagDataDispatcher, MBTagDataDispatcherV2

logger = logging.getLogger(__name__)


messages_v1 = []
messages_v1_lock = asyncio.Lock()  # To synchronize coros accessing messages
messages_v2 = []
messages_v2_lock = asyncio.Lock()

logger = logging.getLogger(__name__)


# ToDo: Move helper functions to a common place once we revisit teh gundi connector / sdk
async def send_observation_to_dead_letter_topic(transformed_observation, attributes):
    with tracing.tracer.start_as_current_span(
            "send_message_to_dead_letter_topic", kind=SpanKind.CLIENT
    ) as current_span:

        print(f"Forwarding observation to dead letter topic: {transformed_observation}")
        # Publish to another PubSub topic
        connect_timeout, read_timeout = settings.DEFAULT_REQUESTS_TIMEOUT
        timeout_settings = aiohttp.ClientTimeout(
            sock_connect=connect_timeout, sock_read=read_timeout
        )
        async with aiohttp.ClientSession(
            raise_for_status=True, timeout=timeout_settings
        ) as session:
            client = pubsub.PublisherClient(session=session)
            # Get the topic
            topic_name = settings.DEAD_LETTER_TOPIC
            current_span.set_attribute("topic", topic_name)
            topic = client.topic_path(settings.GCP_PROJECT_ID, topic_name)
            # Prepare the payload
            binary_payload = json.dumps(transformed_observation, default=str).encode("utf-8")
            messages = [pubsub.PubsubMessage(binary_payload, **attributes)]
            logger.info(f"Sending observation to PubSub topic {topic_name}..")
            try:  # Send to pubsub
                response = await client.publish(topic, messages)
            except Exception as e:
                logger.exception(
                    f"Error sending observation to dead letter topic {topic_name}: {e}. Please check if the topic exists or review settings."
                )
                raise e
            else:
                logger.info(f"Observation sent to the dead letter topic successfully.")
                logger.debug(f"GCP PubSub response: {response}")

        current_span.set_attribute("is_sent_to_dead_letter_queue", True)
        current_span.add_event(
            name="mb_dispatcher.observation_sent_to_dead_letter_queue"
        )


# ToDo: Retry with backoff?
async def send_data_v1_to_movebank(tag_data, tag_id, outbound_config_id):
    with tracing.tracer.start_as_current_span(
            "mb_dispatcher.send_data_v1_to_movebank", kind=SpanKind.CLIENT
    ) as current_span:
        try:
            extra_dict = {
                ExtraKeys.Observation: tag_data,
                ExtraKeys.DeviceId: tag_id,
                ExtraKeys.OutboundIntId: outbound_config_id,
            }
            logger.info(
                "Dispatching observations.",
                extra=extra_dict
            )

            if not outbound_config_id:
                logger.error(
                    "send_data_v1_to_movebank - value error: No outbound config id.",
                    extra=extra_dict,
                )
                raise ReferenceDataError
            # Get details about the destination
            config = await get_outbound_config_detail(outbound_config_id)
            if not config:
                logger.error(
                    f"send_data_v1_to_movebank - No outbound config detail found",
                    extra={**extra_dict, ExtraKeys.AttentionNeeded: True},
                )
                raise ReferenceDataError

            try:  # Send the data
                dispatcher = MBTagDataDispatcher(config)
                mb_messages = [m["data"] for m in tag_data]
                await dispatcher.send(messages=mb_messages, tag=tag_id)
            except Exception as e:
                logger.error(
                    f"Exception occurred dispatching observation",
                    extra={
                        **extra_dict,
                        ExtraKeys.AttentionNeeded: True,
                    },
                )
                raise DispatcherException(f"Exception occurred dispatching observation: {e}")
            else:
                current_span.set_attribute("is_dispatched_successfully", True)
                current_span.set_attribute("destination_id", str(outbound_config_id))
                current_span.add_event(
                    name="mb_dispatcher.observation_dispatched_successfully"
                )
        except (DispatcherException, ReferenceDataError) as e:
            logger.exception(
                f"External error occurred processing transformed observation",
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    **extra_dict
                },
            )
            # Raise the exception so the function execution is marked as failed and retried later
            raise e
        except Exception as e:
            error_msg = (
                f"Unexpected internal error occurred processing transformed observation: {e}"
            )
            logger.exception(
                error_msg,
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.DeadLetter: True,
                    **extra_dict
                },
            )
            # Unexpected internal errors will be redirected straight to deadletter
            current_span.set_attribute("error", error_msg)
            # Send observatios to a dead letter pub/sub topic
            for message in tag_data:
                transformed_observation = message["data"]
                attributes = message["attributes"]
                await send_observation_to_dead_letter_topic(transformed_observation, attributes)


def group_messages_by_tag_id(messages):
    return groupby(
        sorted(messages, key=lambda m: m["data"]["tag_id"]),
        key=lambda m: m["data"]["tag_id"]
    )


def group_messages_by_attribute(messages, attribute):
    return groupby(
        sorted(messages, key=lambda m: m["attributes"][attribute]),
        key=lambda m: m["attributes"][attribute]
    )


# ToDo: Retry with backoff?
async def process_batch_v1(messages: list):
    # Group by tag id
    messages_grouped_by_tag = group_messages_by_tag_id(messages=messages)
    # Process each group serially to avoid too many concurrent requests to Movebank
    for tag, data_iterator in messages_grouped_by_tag:
        tag_data = list(data_iterator)
        messages_grouped_by_destination = group_messages_by_attribute(
            messages=tag_data,
            attribute="outbound_config_id"
        )
        # Group by destination as each one may have different configurations
        for config_id, dest_data_iterator in messages_grouped_by_destination:
            tag_data_by_destination = list(dest_data_iterator)
            timestamp = datetime.now()
            print(
                f"[{timestamp}] Sending messages for tag {tag} and dest {config_id} to Movebank.."
            )
            await send_data_v1_to_movebank(
                tag_data=tag_data_by_destination,
                tag_id=tag,
                outbound_config_id=config_id
            )


# ToDo: Retry with backoff?
async def send_data_v2_to_movebank(tag_data, tag_id, destination_id):
    with tracing.tracer.start_as_current_span(
            "mb_dispatcher.send_data_v2_to_movebank", kind=SpanKind.CLIENT
    ) as current_span:
        try:
            extra_dict = {
                ExtraKeys.Observation: tag_data,
                ExtraKeys.DeviceId: tag_id,
                ExtraKeys.OutboundIntId: destination_id,
            }
            logger.info(
                "Dispatching observations.",
                extra=extra_dict
            )

            if not destination_id:
                logger.error(
                    "send_data_v2_to_movebank - value error: No outbound config id.",
                    extra=extra_dict,
                )
                raise ReferenceDataError
            # Get details about the destination
            destination_integration = await get_integration_details(integration_id=destination_id)
            if not destination_integration:
                logger.error(
                    f"No destination config details found",
                    extra={**extra_dict, ExtraKeys.AttentionNeeded: True},
                )
                raise ReferenceDataError

            try:  # Send the data
                dispatcher = MBTagDataDispatcherV2(destination_integration)
                mb_messages = [m["data"] for m in tag_data]
                await dispatcher.send(messages=mb_messages, tag=tag_id)
            except Exception as e:
                logger.error(
                    f"Exception occurred dispatching observation",
                    extra={
                        **extra_dict,
                        ExtraKeys.AttentionNeeded: True,
                    },
                )
                # Emit events for the portal and other interested services (EDA)
                for observation in tag_data:
                    gundi_id = observation["attributes"]["gundi_id"]
                    related_to = observation["attributes"]["related_to"]
                    data_provider_id = observation["attributes"]["data_provider_id"]
                    await publish_event(
                        event=system_events.ObservationDeliveryFailed(
                            payload=gundi_schemas_v2.DispatchedObservation(
                                gundi_id=gundi_id,
                                related_to=related_to,
                                external_id=None,  # ID returned by the destination system
                                data_provider_id=data_provider_id,
                                destination_id=destination_id,
                                delivered_at=datetime.now(timezone.utc)  # UTC
                            )
                        ),
                        topic_name=settings.DISPATCHER_EVENTS_TOPIC
                    )
                    raise DispatcherException(f"Exception occurred dispatching observation: {e}")
            else:
                for observation in tag_data:
                    gundi_id = observation["attributes"]["gundi_id"]
                    related_to = observation["attributes"]["related_to"]
                    data_provider_id = observation["attributes"]["data_provider_id"]
                    # Cache data related to the dispatched observation
                    dispatched_observation = gundi_schemas_v2.DispatchedObservation(
                        gundi_id=gundi_id,
                        related_to=related_to,
                        external_id=None,  # Movebank API doesn't return any ID
                        data_provider_id=data_provider_id,
                        destination_id=destination_id,
                        delivered_at=datetime.now(timezone.utc)  # UTC
                    )
                    cache_dispatched_observation(observation=dispatched_observation)
                    # Emit events for the portal and other interested services (EDA)
                    await publish_event(
                        event=system_events.ObservationDelivered(
                            payload=dispatched_observation
                        ),
                        topic_name=settings.DISPATCHER_EVENTS_TOPIC
                    )
        except (DispatcherException, ReferenceDataError) as e:
            logger.exception(
                f"External error occurred processing transformed observation",
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    **extra_dict
                },
            )
            # Raise the exception so the function execution is marked as failed and retried later
            raise e
        except Exception as e:
            error_msg = (
                f"Unexpected internal error occurred processing transformed observation: {e}"
            )
            logger.exception(
                error_msg,
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.DeadLetter: True,
                    **extra_dict
                },
            )
            # Unexpected internal errors will be redirected straight to deadletter
            current_span.set_attribute("error", error_msg)
            # Send observatios to a dead letter pub/sub topic
            for message in tag_data:
                transformed_observation = message["data"]
                attributes = message["attributes"]
                await send_observation_to_dead_letter_topic(transformed_observation, attributes)


# ToDo: Retry with backoff?
async def process_batch_v2(messages: list):
    # Group by tag id
    messages_grouped_by_tag = group_messages_by_tag_id(messages=messages)
    # Process each group serially to avoid too many concurrent requests to Movebank
    for tag, data_iterator in messages_grouped_by_tag:
        tag_data = list(data_iterator)
        messages_grouped_by_destination = group_messages_by_attribute(
            messages=tag_data,
            attribute="destination_id"
        )
        # Group by destination as each one may have different configurations
        for config_id, dest_data_iterator in messages_grouped_by_destination:
            tag_data_by_destination = list(dest_data_iterator)
            timestamp = datetime.now()
            print(
                f"[{timestamp}] Sending messages for tag {tag} and dest {config_id} to Movebank.."
            )
            await send_data_v2_to_movebank(
                tag_data=tag_data_by_destination,
                tag_id=tag,
                destination_id=config_id
            )


async def process_observation_v2(observation):
    global messages_v2
    global messages_v2_lock
    transformed_observation = observation["data"]
    attributes = observation["attributes"]
    with tracing.tracer.start_as_current_span(
            "mb_dispatcher.process_observation_v2", kind=SpanKind.CLIENT
    ) as current_span:
        current_span.set_attribute("transformed_message", str(transformed_observation))
        current_span.set_attribute("environment", settings.TRACE_ENVIRONMENT)
        current_span.set_attribute("service", "mb-dispatcher")
        observation_type = attributes.get("observation_type")
        source_id = attributes.get("device_id")
        provider_id = attributes.get("data_provider_id")
        destination_id = attributes.get("destination_id")
        retry_attempt: int = attributes.get("retry_attempt") or 0
        logger.debug(f"transformed_observation: {transformed_observation}")
        logger.info(
            "received transformed observation",
            extra={
                ExtraKeys.DeviceId: source_id,
                ExtraKeys.InboundIntId: provider_id,
                ExtraKeys.OutboundIntId: destination_id,
                ExtraKeys.StreamType: observation_type,
                ExtraKeys.RetryAttempt: retry_attempt,
            },
        )
        # Buffer messages to process them in batches
        messages_to_process = None
        now_dt = datetime.now()
        # print(f"[{now_dt}] Message received: \n {message}")
        async with messages_v2_lock:
            messages_v2.append(observation)
            if len(messages_v1) >= settings.MAX_MESSAGES_PER_FILE:
                print(f"[{now_dt}] {len(messages_v2)} messages reached. Flushing bufer")
                messages_to_process = messages_v2.copy()
                messages_v2.clear()
        if messages_to_process:
            await process_batch_v2(messages=messages_to_process)


async def process_observation_v1(observation):
    global messages_v1
    global messages_v1_lock
    transformed_observation = observation["data"]
    attributes = observation["attributes"]
    with tracing.tracer.start_as_current_span(
            "mb_dispatcher.process_observation_v1", kind=SpanKind.CLIENT
    ) as current_span:
        current_span.set_attribute("transformed_message", str(transformed_observation))
        current_span.set_attribute("environment", settings.TRACE_ENVIRONMENT)
        current_span.set_attribute("service", "mb-dispatcher")
        observation_type = attributes.get("observation_type")
        device_id = attributes.get("device_id")
        integration_id = attributes.get("integration_id")
        outbound_config_id = attributes.get("outbound_config_id")
        retry_attempt: int = attributes.get("retry_attempt") or 0
        logger.debug(f"transformed_observation: {transformed_observation}")
        logger.info(
            "received transformed observation",
            extra={
                ExtraKeys.DeviceId: device_id,
                ExtraKeys.InboundIntId: integration_id,
                ExtraKeys.OutboundIntId: outbound_config_id,
                ExtraKeys.StreamType: observation_type,
                ExtraKeys.RetryAttempt: retry_attempt,
            },
        )
        # Buffer messages to process them in batches
        messages_to_process = None
        now_dt = datetime.now()
        # print(f"[{now_dt}] Message received: \n {message}")
        async with messages_v1_lock:
            messages_v1.append(observation)
            if len(messages_v1) >= settings.MAX_MESSAGES_PER_FILE:
                print(f"[{now_dt}] {len(messages_v1)} messages reached. Flushing bufer")
                messages_to_process = messages_v1.copy()
                messages_v1.clear()
        if messages_to_process:
            await process_batch_v1(messages=messages_to_process)


async def process_message(message):
    # Start Tracing & Logging for troubleshooting
    with tracing.tracer.start_as_current_span(
            "mb_dispatcher.process_transformed_observation", kind=SpanKind.CLIENT
    ) as current_span:
        current_span.add_event(
            name="mb_dispatcher.transformed_observation_received_at_dispatcher"
        )
        # Extract message content
        observation = {
            "data": json.loads(message.data),
            "attributes": message.attributes
        }
        if message.attributes.get("gundi_version", "v1") == "v1":
            await process_observation_v1(observation=observation)
        else:
            await process_observation_v2(observation=observation)


async def flush_messages_v1():
    global messages_v1
    global messages_v1_lock
    global messages_v2
    global messages_v2_lock
    now_dt = datetime.now()
    # Flush messages from Gundi v1
    async with messages_v1_lock:
        messages_to_process = messages_v1.copy()
        messages_v1.clear()
    if messages_to_process:
        print(f"[{now_dt}] Flushing messages v1. {len(messages_v1)} messages in buffer. ")
        await process_batch_v1(messages=messages_to_process)


async def flush_messages_v2():
    global messages_v2
    global messages_v2_lock
    now_dt = datetime.now()
    # Flush messages from Gundi v2
    async with messages_v2_lock:
        messages_to_process = messages_v2.copy()
        messages_v2.clear()
    if messages_to_process:
        print(f"[{now_dt}] Flushing messages v2. {len(messages_v2)} messages in buffer. ")
        await process_batch_v2(messages=messages_to_process)


async def consume_messages():
    subscription_path = f"projects/{settings.GCP_PROJECT_ID}/subscriptions/{settings.TRANSFORMED_OBSERVATIONS_SUB_ID}"
    print(f"Consuming messages from: \n {subscription_path}")
    async with pubsub.SubscriberClient() as subscriber_client:
        await pubsub.subscribe(
            subscription_path,
            process_message,
            subscriber_client,
            num_producers=settings.PULL_CONCURRENCY,
            max_messages_per_producer=settings.BATCH_MAX_MESSAGES,
            ack_window=0.3,
            num_tasks_per_consumer=settings.BATCH_MAX_MESSAGES,
            enable_nack=True,
            nack_window=0.3,
        )
    print(f"Consumer stopped.")

