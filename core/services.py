import asyncio
import datetime
import json
import logging
from itertools import groupby
from gcloud.aio.pubsub import SubscriberClient, subscribe
from . import settings


logger = logging.getLogger(__name__)


messages = []
lock = asyncio.Lock()  # To synchronize coros accessing messages


async def process_message(message):
    global messages
    global lock
    messages_to_process = None
    now_dt = datetime.datetime.now()
    # print(f"[{now_dt}] Message received: \n {message}")
    async with lock:
        messages.append(message)
        if len(messages) >= settings.MAX_MESSAGES_PER_FILE:
            print(f"[{now_dt}] {len(messages)} messages reached. Flushing bufer")
            messages_to_process = messages.copy()
            messages.clear()
    if messages_to_process:
        await process_batch(messages=messages_to_process)
    return


async def flush_messages():
    global messages
    global lock
    now_dt = datetime.datetime.now()
    async with lock:
        print(f"[{now_dt}] Flushing messages. {len(messages)} messages in buffer. ")
        messages_to_process = messages.copy()
        messages.clear()
    if messages_to_process:
        await process_batch(messages=messages_to_process)
    return


async def process_batch(messages):
    # Binary to dict
    message_list = [json.loads(m.data) for m in messages]
    messages_grouped_by_tag = groupby(
        sorted(message_list, key=lambda m: m["tag_id"]),
        key=lambda m: m["tag_id"]
    )
    # Process them serially to avoid too many concurrent requests to Movebank
    for tag, iterator in messages_grouped_by_tag:
        timestamp = datetime.datetime.now()
        print(
            f"[{timestamp}] Sending messages for tag {tag} to Movebank.."
        )


async def consume_messages():
    subscription_path = f"projects/{settings.GCP_PROJECT_ID}/subscriptions/{settings.TRANSFORMED_OBSERVATIONS_SUB_ID}"
    print(f"Consuming messages from: \n {subscription_path}")
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

