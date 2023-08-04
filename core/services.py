import asyncio
import logging
from gcloud.aio.pubsub import SubscriberClient, SubscriberMessage
from . import settings


logger = logging.getLogger(__name__)


async def consume_messages():
    async with SubscriberClient() as client:
        # pull messages
        subscription_path = f"projects/{settings.GCP_PROJECT_ID}/subscriptions/{settings.TRANSFORMED_OBSERVATIONS_SUB_ID}"
        while True:
            pull_tasks = []
            for i in range(settings.CONCURRENCY):
                pull_tasks.append(
                    asyncio.ensure_future(
                        client.pull(
                            subscription_path,
                            max_messages=settings.BATCH_MAX_MESSAGES,  # Pull up to BATCH_MAX_MESSAGES from the topic
                            timeout=settings.BATCH_READ_TIMEOUT  # seconds
                        )
                    )
                )

            results = await asyncio.gather(*pull_tasks, return_exceptions=True)
            messages = [m for m in results if isinstance(m, SubscriberMessage)]

            # Process messages if any
            if not messages:
                print(f"No messages to process. Continue")
                continue

            print(f"Messages Received: \n{messages}")
            # ToDo: Send messages to Movebank
            # ToDo: Acknowledge processed messages
