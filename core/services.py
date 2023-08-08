import datetime
import logging
from google.cloud import pubsub_v1
from . import settings


logger = logging.getLogger(__name__)


messages = []
last_sending_dt = datetime.datetime.now()


def process_message(message):
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
    message.ack()
    return


def consume_messages():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        settings.GCP_PROJECT_ID,
        settings.TRANSFORMED_OBSERVATIONS_SUB_ID
    )
    print(f"[{last_sending_dt}] Consuming messages from: \n {subscription_path}")
    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=process_message
    )
    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            streaming_pull_future.result()
        except Exception as e:
            logger.error(f"Internal Error {e}. Shutting down..\n")
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.
    print(f"Consumer stopped.")

