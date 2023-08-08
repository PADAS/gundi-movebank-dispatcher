import asyncio
import logging
#from core import tracing
from core.services import consume_messages

logger = logging.getLogger(__name__)


if __name__ == '__main__':
    # Wrapper to be able to run the async function
    asyncio.run(consume_messages())
