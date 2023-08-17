# ToDo: Move base classes or utils into the SDK
import logging
import json
import aiofiles
from abc import ABC, abstractmethod
from erclient import AsyncERClient
from typing import Union, List
from gundi_core import schemas
from cdip_connector.core.cloudstorage import get_cloud_storage
from movebank_client import MovebankClient
from core.utils import find_config_for_action

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = (3.1, 20)


class Dispatcher(ABC):
    stream_type: schemas.StreamPrefixEnum
    destination_type: schemas.DestinationTypes

    def __init__(self, config: schemas.OutboundConfiguration):
        self.configuration = config

    @abstractmethod
    async def send(self, messages: list, **kwargs):
        ...


class MBDispatcher(Dispatcher, ABC):
    DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0

    def __init__(self, config: schemas.OutboundConfiguration):
        super().__init__(config)
        self.mb_client = self.make_mb_client(config)

    @staticmethod
    def make_mb_client(
        config: schemas.OutboundConfiguration,
    ) -> MovebankClient:
        return MovebankClient(
            base_url=config.endpoint,
            username=config.login,
            password=config.password,
            connect_timeout=MBDispatcher.DEFAULT_CONNECT_TIMEOUT_SECONDS,
        )


class MBTagDataDispatcher(MBDispatcher):
    def __init__(self, config):
        super().__init__(config)

    async def send(self, messages: Union[list, dict], **kwargs):
        result = None
        if isinstance(messages, dict):
            messages = [messages]
        feed = self.configuration.additional.get("feed")
        tag = kwargs.get("tag")
        async with aiofiles.tempfile.TemporaryFile(mode='w+b', prefix="movebank_data", suffix=".json") as data_file:
            await data_file.write(json.dumps(messages).encode('utf-8'))
            await data_file.seek(0)  # Rewind fp
            async with self.mb_client as client:
                result = await client.post_tag_data(
                    feed_name=feed,
                    tag_id=tag,
                    json_file=data_file
                )
        return result


class MBDispatcherV2(ABC):
    stream_type: schemas.v2.StreamPrefixEnum
    destination_type: schemas.DestinationTypes

    def __init__(
            self,
            integration: schemas.v2.Integration,
            **kwargs
    ):
        self.integration = integration

    @abstractmethod
    async def send(self, messages: list, **kwargs):
        ...


class MBTagDataDispatcherV2(MBDispatcherV2):
    def __init__(
            self,
            integration: schemas.v2.Integration,
            **kwargs
    ):
        super().__init__(integration=integration, **kwargs)

    async def send(self, messages: list, **kwargs):
        # ToDo: Implement
        pass

