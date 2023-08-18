import asyncio
import logging
from core import tracing
from core.services import consume_messages, flush_messages_v1, flush_messages_v2
from core.utils import periodic_task

logger = logging.getLogger(__name__)


async def main():
    await asyncio.gather(
        consume_messages(),
        periodic_task(interval=1, func=flush_messages_v1),
        periodic_task(interval=1, func=flush_messages_v2)
    )


if __name__ == '__main__':
    # Wrapper to be able to run the async function
    asyncio.run(main())
