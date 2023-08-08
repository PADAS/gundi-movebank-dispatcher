import logging
#from core import tracing
from core.services import consume_messages

logger = logging.getLogger(__name__)


if __name__ == '__main__':
    consume_messages()
