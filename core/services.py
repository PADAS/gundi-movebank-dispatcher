import asyncio
import datetime
import logging
from gcloud.aio.pubsub import SubscriberClient, SubscriberMessage, subscribe
from . import settings


logger = logging.getLogger(__name__)


messages = []
last_sending_dt = datetime.datetime.now()


async def process_message(message):
    global messages
    global last_sending_dt
    now_dt = datetime.datetime.now()
    print(f"[{now_dt}] Message received: \n {message}")
    messages.append(message)
    time_since_last_sending = (now_dt - last_sending_dt).total_seconds()
    if len(messages) >= settings.MESSAGES_PER_FILE:
        last_sending_dt = datetime.datetime.now()
        print(
            f"[{last_sending_dt}] Sending {len(messages)} messages to Movebank (after {time_since_last_sending} secs).."
        )
        messages.clear()
    return


async def consume_messages():
    subscription_path = f"projects/{settings.GCP_PROJECT_ID}/subscriptions/{settings.TRANSFORMED_OBSERVATIONS_SUB_ID}"
    print(f"[{last_sending_dt}] Consuming messages from: \n {subscription_path}")
    async with SubscriberClient() as subscriber_client:
        await subscribe(
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

