import logging.config
import sys
from environs import Env

env = Env()
env.read_env()

LOGGING_LEVEL = env.str("LOGGING_LEVEL", "INFO")

DEFAULT_LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {
            "format": "%(asctime)s %(levelname)s %(processName)s %(thread)d %(name)s %(message)s",
            "class": "pythonjsonlogger.jsonlogger.JsonFormatter",
        },
    },
    "handlers": {
        "console": {
            "level": LOGGING_LEVEL,
            "class": "logging.StreamHandler",
            "stream": sys.stdout,
            "formatter": "json",
        },
    },
    "loggers": {
        "": {
            "handlers": ["console"],
            "level": LOGGING_LEVEL,
        },
    },
}
logging.config.dictConfig(DEFAULT_LOGGING)

DEFAULT_REQUESTS_TIMEOUT = (10, 20)  # Connect, Read

CDIP_API_ENDPOINT = env.str("CDIP_API_ENDPOINT", None)
CDIP_ADMIN_ENDPOINT = env.str("CDIP_ADMIN_ENDPOINT", None)
PORTAL_API_ENDPOINT = f"{CDIP_ADMIN_ENDPOINT}/api/v1.0"
PORTAL_OUTBOUND_INTEGRATIONS_ENDPOINT = (
    f"{PORTAL_API_ENDPOINT}/integrations/outbound/configurations"
)
PORTAL_INBOUND_INTEGRATIONS_ENDPOINT = (
    f"{PORTAL_API_ENDPOINT}/integrations/inbound/configurations"
)

# Settings for caching admin portal request/responses
REDIS_HOST = env.str("REDIS_HOST", "localhost")
REDIS_PORT = env.int("REDIS_PORT", 6379)
REDIS_DB = env.int("REDIS_DB", 3)

# N-seconds to cache portal responses for configuration objects.
PORTAL_CONFIG_OBJECT_CACHE_TTL = env.int("PORTAL_CONFIG_OBJECT_CACHE_TTL", 60)
DISPATCHED_OBSERVATIONS_CACHE_TTL = env.int("PORTAL_CONFIG_OBJECT_CACHE_TTL", 60 * 60)  # 1 Hour

# Used in OTel traces/spans to set the 'environment' attribute, used on metrics calculation
TRACE_ENVIRONMENT = env.str("TRACE_ENVIRONMENT", "dev")

# GCP related settings
GCP_PROJECT_ID = env.str("GCP_PROJECT_ID", "cdip-78ca")
DEAD_LETTER_TOPIC = env.str("DEAD_LETTER_TOPIC", "destinations-dead-letter-stage")
DISPATCHER_EVENTS_TOPIC = env.str("DISPATCHER_EVENTS_TOPIC", "dispatcher-events-stage")
TRANSFORMED_OBSERVATIONS_SUB_ID = env.str("TRANSFORMED_OBSERVATIONS_SUB_ID")

# Message processing
PULL_MAX_MESSAGES = env.int("PULL_MAX_MESSAGES", 20)  # Messages pulled by each producer
PULL_CONCURRENCY = env.int("PULL_CONCURRENCY", 5)  # Messages pulled by each producer
BATCH_MAX_MESSAGES = env.int("BATCH_MAX_MESSAGES", 100)  # Max messages to be sent to Movebank in a BATCH
FLUSH_TIMEOUT_SECONDS = env.int("FLUSH_TIMEOUT_SECONDS", 1)  # After this time we send the messages we got so far
MAX_TIME_RETRIES_SECONDS = env.int("MAX_TIME_RETRIES_SECONDS", 86400)   # 24hrs
