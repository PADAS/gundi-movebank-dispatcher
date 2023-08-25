# ToDo: Move base classes or utils into the SDK
import logging
import json
import aiofiles
import pydantic
from abc import ABC, abstractmethod
from gundi_core import schemas
from movebank_client import MovebankClient
from .utils import find_config_for_action, ExtraKeys
from .errors import ReferenceDataError

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


async def send_data_to_movebank(mb_client: MovebankClient, messages: list, feed: str, tag_id: str):
    async with aiofiles.tempfile.TemporaryFile(mode='w+b', prefix="movebank_data", suffix=".json") as data_file:
        await data_file.write(json.dumps(messages).encode('utf-8'))
        await data_file.seek(0)  # Rewind fp
        async with mb_client as client:
            result = await client.post_tag_data(
                feed_name=feed,
                tag_id=tag_id,
                json_file=data_file
            )
    return result


class MBTagDataDispatcher(MBDispatcher):
    def __init__(self, config):
        super().__init__(config)

    async def send(self, messages: list, **kwargs):
        feed = self.configuration.additional.get("feed")
        if not feed:
            error_msg = f"`feed` is missing in the outbound configuration (additional), id {str(self.configuration.id)}. Please fix the integration setup in the portal."
            logger.error(
                error_msg,
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.OutboundIntId: str(self.configuration.id)
                }
            )
            raise ReferenceDataError(error_msg)
        tag = kwargs.get("tag")
        return await send_data_to_movebank(
            mb_client=self.mb_client,
            messages=messages,
            feed=feed,
            tag_id=tag
        )


class MBDispatcherV2(ABC):
    stream_type: schemas.v2.StreamPrefixEnum
    destination_type: schemas.DestinationTypes

    def __init__(
            self,
            integration: schemas.v2.Integration,
            **kwargs
    ):
        self.integration = integration
        self.mb_client = self.make_mb_client(self.integration)


    @staticmethod
    def make_mb_client(
        integration: schemas.v2.Integration,
    ) -> MovebankClient:
        # Look for the configuration of the authentication action
        configurations = integration.configurations
        action_value = schemas.v2.MovebankActions.AUTHENTICATE.value
        auth_action_config = find_config_for_action(
            configurations=configurations,
            action_value=action_value
        )
        if not auth_action_config:
            error_msg = f"{action_value}` action configuration for integration {str(integration.id)} is missing. Please fix the integration setup in the portal."
            logger.error(
                error_msg,
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.OutboundIntId: str(integration.id)
                }
            )
            raise ReferenceDataError(error_msg)
        try:
            auth_config = schemas.v2.MBAuthActionConfig.parse_obj(auth_action_config.data)
        except pydantic.ValidationError:
            error_msg = f"Invalid configuration for Movebank action `{action_value}`. Integration id: {integration.id}."
            logger.error(
                error_msg,
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.OutboundIntId: str(integration.id)
                }
            )
            raise ReferenceDataError(error_msg)
        return MovebankClient(
            base_url=integration.base_url,
            username=auth_config.username,
            password=auth_config.password,
            connect_timeout=MBDispatcher.DEFAULT_CONNECT_TIMEOUT_SECONDS,
        )

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
        # Look for the configuration of the authentication action
        configurations = self.integration.configurations
        action_value = schemas.v2.MovebankActions.PUSH_OBSERVATIONS.value
        push_obs_action_config = find_config_for_action(
            configurations=configurations,
            action_value=action_value
        )
        if not push_obs_action_config:
            error_msg = f"`{action_value}` action configuration for integration {str(self.integration.id)} is missing. Please fix the integration setup in the portal."
            logger.error(
                error_msg,
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.OutboundIntId: str(self.integration.id)
                }
            )
            raise ReferenceDataError(error_msg)
        try:
            push_config = schemas.v2.MBPushObservationsActionConfig.parse_obj(push_obs_action_config.data)
        except pydantic.ValidationError:
            error_msg = f"Invalid configuration for Movebank action `{action_value}`. Integration id: {self.integration.id}."
            logger.error(
                error_msg,
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.OutboundIntId: str(self.integration.id)
                }
            )
            raise ReferenceDataError(error_msg)
        feed = push_config.feed
        tag = kwargs.get("tag")
        return await send_data_to_movebank(
            mb_client=self.mb_client,
            messages=messages,
            feed=feed,
            tag_id=tag
        )
