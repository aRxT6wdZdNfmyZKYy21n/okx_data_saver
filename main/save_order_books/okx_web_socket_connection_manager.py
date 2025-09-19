import asyncio
import logging
from collections import (
    defaultdict,
)
from decimal import (
    Decimal,
)

import orjson
import traceback
import typing
import uuid

import websockets.asyncio.client
from aiogram.utils.text_decorations import (
    markdown_decoration,
)

from event.async_ import AsyncEvent
from main.save_order_books.constants_ import (
    IS_NEED_SEND_NOTIFICATIONS_ABOUT_WEB_SOCKET_CONNECTION_CLOSED_WITH_ERROR,
    USE_PROXIES,
)
from utils.proxy import (
    ProxyUtils,
)
from utils.telegram import (
    TelegramUtils,
)
from utils.time import TimeUtils

logger = logging.getLogger(
    __name__,
)


class _WebSocketConnectionTimeoutError(Exception):
    pass


class OKXWebSocketConnectionManager(object):
    __slots__ = (
        '__ask_quantity_by_price_map_by_symbol_name_map',
        '__bid_quantity_by_price_map_by_symbol_name_map',
        '__on_new_order_book_data_event',
        '__process_idx',
        '__subscribed_symbol_name_set',
        '__web_socket_connection',
        '__web_socket_connection_idx',
        '__web_socket_connection_creation_event',
        '__web_socket_connection_stats_raw_data',
        '__web_socket_connections_count_per_process',
    )

    def __init__(
        self,
        process_idx: int,
        web_socket_connection_idx: int,
        web_socket_connections_count_per_process: int,
    ) -> None:
        super().__init__()

        self.__ask_quantity_by_price_map_by_symbol_name_map: (
            typing.DefaultDict[str, dict[Decimal, Decimal]] | None
        ) = None

        self.__bid_quantity_by_price_map_by_symbol_name_map: (
            typing.DefaultDict[str, dict[Decimal, Decimal]] | None
        ) = None

        self.__on_new_order_book_data_event = AsyncEvent(
            'OnNewOrderBookRawDataEvent',
        )

        self.__process_idx = process_idx
        self.__subscribed_symbol_name_set: set[str] = set()
        self.__web_socket_connection: (
            websockets.asyncio.client.ClientConnection | None
        ) = None
        self.__web_socket_connection_creation_event: asyncio.Event = asyncio.Event()
        self.__web_socket_connection_idx = web_socket_connection_idx
        self.__web_socket_connection_stats_raw_data: dict[str, typing.Any] | None = None
        self.__web_socket_connections_count_per_process = (
            web_socket_connections_count_per_process
        )

    def get_ask_quantity_by_price_map(
        self,
        symbol_name: str,
    ) -> dict[Decimal, Decimal] | None:
        ask_quantity_by_price_map_by_symbol_name_map = (
            self.__ask_quantity_by_price_map_by_symbol_name_map
        )

        if ask_quantity_by_price_map_by_symbol_name_map is None:
            return None

        return ask_quantity_by_price_map_by_symbol_name_map.get(
            symbol_name,
        )

    def get_bid_quantity_by_price_map(
        self,
        symbol_name: str,
    ) -> dict[Decimal, Decimal] | None:
        bid_quantity_by_price_map_by_symbol_name_map = (
            self.__bid_quantity_by_price_map_by_symbol_name_map
        )

        if bid_quantity_by_price_map_by_symbol_name_map is None:
            return None

        return bid_quantity_by_price_map_by_symbol_name_map.get(
            symbol_name,
        )

    def get_on_new_order_book_data_event(
        self,
    ) -> AsyncEvent:
        return self.__on_new_order_book_data_event

    async def start_loop(
        self,
    ) -> None:
        while True:
            web_socket_connection: websockets.asyncio.client.ClientConnection | None = (
                self.__web_socket_connection
            )
            if web_socket_connection is None:
                await self.__web_socket_connection_creation_event.wait()

                web_socket_connection = self.__web_socket_connection

            assert web_socket_connection is not None, None

            tasks = (
                # asyncio.ensure_future(
                #     self.__start_pinging_loop()
                # ),
                asyncio.ensure_future(
                    self.__start_receiving_loop(),
                ),
            )

            try:
                await asyncio.gather(
                    *tasks,
                )

                await web_socket_connection.close()
            except _WebSocketConnectionTimeoutError:
                logger.warning(
                    'OKX Spot order books WebSocket connection was timed out'
                )

                await TelegramUtils.send_message_to_channel(
                    message_markdown_text=markdown_decoration.quote(
                        'OKX Spot order books WebSocket connection was timed out',
                    )
                )

                for task in tasks:
                    task.cancel()

                try:
                    await web_socket_connection.close()
                except Exception as exception:
                    logger.error(
                        'Could not close OKX Spot WebSocket connection'
                        ': handled exception'
                        f': {"".join(traceback.format_exception(exception))}'
                    )
            except websockets.exceptions.ConnectionClosedOK:
                logger.info(
                    'OKX Spot order books WebSocket connection was closed (OK)',
                )

                for task in tasks:
                    task.cancel()
            except websockets.exceptions.ConnectionClosedError:
                logger.error(
                    'OKX Spot order books WebSocket connection was closed (with error)'
                    '. Web socket connection stats'
                    f': {self.__web_socket_connection_stats_raw_data}'
                )

                if IS_NEED_SEND_NOTIFICATIONS_ABOUT_WEB_SOCKET_CONNECTION_CLOSED_WITH_ERROR:
                    await TelegramUtils.send_message_to_channel(
                        message_markdown_text=markdown_decoration.quote(
                            'OKX Spot order books WebSocket connection was closed (with error)'
                            '. Web socket connection stats'
                            f': {self.__web_socket_connection_stats_raw_data}',
                        )
                    )

                for task in tasks:
                    task.cancel()
            except Exception as exception:
                logger.error(
                    'Handled exception in OKX Spot order books loops'
                    f': {"".join(traceback.format_exception(exception))}'
                    '. Web socket connection stats'
                    f': {self.__web_socket_connection_stats_raw_data}'
                )

                await TelegramUtils.send_message_to_channel(
                    message_markdown_text=markdown_decoration.quote(
                        'Handled exception in OKX Spot order books loops'
                        f': {"".join(traceback.format_exception(exception))}'
                        '. Web socket connection stats'
                        f': {self.__web_socket_connection_stats_raw_data}',
                    )
                )

                for task in tasks:
                    task.cancel()

                try:
                    await web_socket_connection.close()
                except Exception as exception:
                    logger.error(
                        'Could not close OKX Spot WebSocket connection'
                        ': handled exception'
                        f': {"".join(traceback.format_exception(exception))}'
                    )

            self.__web_socket_connection = web_socket_connection = None

            await self.__connect()

    async def subscribe(
        self,
        symbol_name: str,
    ) -> None:
        subscribed_symbol_name_set = self.__subscribed_symbol_name_set
        if symbol_name in subscribed_symbol_name_set:
            return

        await self.__connect()

        await self.__subscribe(
            symbol_name,
        )

        subscribed_symbol_name_set.add(
            symbol_name,
        )

    async def __connect(
        self,
    ) -> None:
        web_socket_connection = self.__web_socket_connection

        if web_socket_connection is not None:
            return

        process_idx = self.__process_idx

        socks5_proxy_urls = ProxyUtils.get_socks5_proxy_urls(
            socks5_proxies_file_names=[
                # 'proxies_ru_proxyline.txt',
                # 'proxies_ru_getproxy.txt',
            ],
        )

        web_socket_connection_idx = self.__web_socket_connection_idx
        web_socket_connections_count_per_process = (
            self.__web_socket_connections_count_per_process
        )

        while True:
            socks5_proxy_url: str | None

            if USE_PROXIES:
                socks5_proxy_idx = (
                    (process_idx * web_socket_connections_count_per_process)
                    + web_socket_connection_idx
                ) % len(socks5_proxy_urls)

                socks5_proxy_url = socks5_proxy_urls[socks5_proxy_idx]
            else:
                socks5_proxy_url = None

            logger.info(
                'Connecting to OKX Spot Web Socket...',
            )

            try:
                web_socket_connection = await websockets.asyncio.client.connect(
                    proxy=socks5_proxy_url,
                    uri='wss://ws.okx.com:8443/ws/v5/public',
                )

                break
            except websockets.exceptions.ProxyError:
                logger.error(
                    'OKX Spot WebSocket connection'
                    ': could not connect to SOCKS5 proxy'
                    f' with URL {socks5_proxy_url}'
                )

                await TelegramUtils.send_message_to_channel(
                    message_markdown_text=markdown_decoration.quote(
                        'OKX Spot WebSocket connection'
                        ': could not connect to SOCKS5 proxy'
                        f' with URL {socks5_proxy_url}'
                    ),
                )

                await asyncio.sleep(
                    5.0,  # s
                )

                continue
            except Exception as exception:
                logger.error(
                    'Handled exception while connecting to OKX Spot WebSocket'
                    f': {"".join(traceback.format_exception(exception))}'
                )

                await TelegramUtils.send_message_to_channel(
                    message_markdown_text=markdown_decoration.quote(
                        'Handled exception while connecting to OKX Spot WebSocket'
                        f': {"".join(traceback.format_exception(exception))}',
                    )
                )

                await asyncio.sleep(
                    15.0,  # s
                )

                continue

        self.__ask_quantity_by_price_map_by_symbol_name_map = defaultdict(
            dict,
        )

        self.__bid_quantity_by_price_map_by_symbol_name_map = defaultdict(
            dict,
        )

        self.__web_socket_connection = web_socket_connection
        self.__web_socket_connection_stats_raw_data = {
            'received_messages_count': 0,
            'received_pings_count': 0,
            'received_pongs_count': 0,
            'sent_pings_count': 0,
            'sent_pongs_count': 0,
            'subscriptions_count': 0,
            'unsubscriptions_count': 0,
        }

        for symbol_idx, symbol_name in enumerate(self.__subscribed_symbol_name_set):
            if symbol_idx:
                await asyncio.sleep(
                    0.5,  # s
                )

            await self.__subscribe(
                symbol_name,
            )

        web_socket_connection_creation_event = (
            self.__web_socket_connection_creation_event
        )

        web_socket_connection_creation_event.set()
        web_socket_connection_creation_event.clear()

    @staticmethod
    def __deserialize_web_socket_message_raw_data(
        message_raw_data_bytes: bytes,
    ) -> dict:
        return orjson.loads(
            message_raw_data_bytes,
        )

    @staticmethod
    def __serialize_web_socket_message_raw_data(
        message_raw_data: dict,
    ) -> str:
        return orjson.dumps(
            message_raw_data,
        ).decode()

    async def __start_pinging_loop(
        self,
    ) -> None:
        while True:
            logger.debug(
                'Pinging...',
            )

            await self.__web_socket_connection.send(
                message=self.__serialize_web_socket_message_raw_data(
                    {
                        'ping': uuid.uuid4().hex,
                        'time': (TimeUtils.get_aware_current_datetime().isoformat()),
                    },
                ),
                text=True,
            )

            web_socket_connection_stats_raw_data = (
                self.__web_socket_connection_stats_raw_data
            )

            (web_socket_connection_stats_raw_data['sent_pings_count']) = (
                (web_socket_connection_stats_raw_data['sent_pings_count']) + 1
            )

            logger.debug(
                'Sent OKX Spot order books WebSocket ping',
            )

            await asyncio.sleep(
                5.0,  # s
            )

    async def __start_receiving_loop(
        self,
    ) -> None:
        ask_quantity_by_price_map_by_symbol_name_map = (
            self.__ask_quantity_by_price_map_by_symbol_name_map
        )

        bid_quantity_by_price_map_by_symbol_name_map = (
            self.__bid_quantity_by_price_map_by_symbol_name_map
        )

        on_new_order_book_data_event = self.__on_new_order_book_data_event

        order_book_sequence_id_by_symbol_name_map: dict[str, int] = {}

        web_socket_connection = self.__web_socket_connection
        web_socket_connection_stats_raw_data = (
            self.__web_socket_connection_stats_raw_data
        )

        while True:
            logger.debug(
                'Listening...',
            )

            try:
                message_raw_data_bytes = await asyncio.wait_for(
                    web_socket_connection.recv(
                        decode=False,
                    ),
                    timeout=(
                        15.0  # s
                    ),
                )
            except TimeoutError:
                logger.warning(
                    'Handled timeout error',
                )

                raise _WebSocketConnectionTimeoutError() from None

            (web_socket_connection_stats_raw_data['received_messages_count']) = (
                (web_socket_connection_stats_raw_data['received_messages_count']) + 1
            )

            message_raw_data = self.__deserialize_web_socket_message_raw_data(
                message_raw_data_bytes,
            )

            event_name: str | None = message_raw_data.pop(
                'event',
                None,
            )

            if event_name is not None:
                assert event_name == 'subscribe', (  # TODO: in ('subscribe', 'notice')
                    event_name,
                    message_raw_data,
                )

                continue

            action_name: str = message_raw_data.pop(
                'action',
            )

            argument_raw_data: dict[str, typing.Any] = message_raw_data.pop(
                'arg',
            )

            channel_name: str = argument_raw_data.pop(
                'channel',
            )

            assert channel_name == 'books', (channel_name,)

            symbol_name: str = argument_raw_data.pop(
                'instId',
            )

            raw_data_list: list[dict[str, typing.Any]] = message_raw_data.pop(
                'data',
            )

            assert len(raw_data_list) == 1, (raw_data_list,)

            raw_data = raw_data_list[0]

            ask_quantity_by_price_map = ask_quantity_by_price_map_by_symbol_name_map[
                symbol_name
            ]

            asks: list[list[str, str, str, str]] = raw_data.pop(
                'asks',
            )

            for price_raw, quantity_raw, _, _ in asks:
                price = Decimal(
                    price_raw,
                )

                quantity = Decimal(
                    quantity_raw,
                )

                if quantity:
                    ask_quantity_by_price_map[price] = quantity
                else:
                    ask_quantity_by_price_map.pop(
                        price,
                        None,
                    )

            bid_quantity_by_price_map = bid_quantity_by_price_map_by_symbol_name_map[
                symbol_name
            ]

            bids: list[list[str, str, str, str]] = raw_data.pop(
                'bids',
            )

            for price_raw, quantity_raw, _, _ in bids:
                price = Decimal(
                    price_raw,
                )

                quantity = Decimal(
                    quantity_raw,
                )

                if quantity:
                    bid_quantity_by_price_map[price] = quantity
                else:
                    bid_quantity_by_price_map.pop(
                        price,
                        None,
                    )

            old_sequence_id = order_book_sequence_id_by_symbol_name_map.get(
                symbol_name,
            )

            new_sequence_id: int = raw_data.pop(
                'seqId',
            )

            previous_sequence_id: int = raw_data.pop(
                'prevSeqId',
            )

            server_timestamp_ms_raw: str = raw_data.pop(
                'ts',
            )

            server_timestamp_ms = int(
                server_timestamp_ms_raw,
            )

            if action_name == 'snapshot':
                assert previous_sequence_id == -1, (previous_sequence_id,)
            elif action_name == 'update':
                assert old_sequence_id is not None, None

                if previous_sequence_id != old_sequence_id:
                    logger.warning(
                        'Message queue was broken'
                        f': previous sequence ID {previous_sequence_id}'
                        ' !='
                        f' old sequence ID {old_sequence_id}'
                    )

                    raise Exception(
                        'Message queue was broken',
                    )
            else:
                raise NotImplementedError(
                    action_name,
                )

            order_book_sequence_id_by_symbol_name_map[symbol_name] = new_sequence_id

            logger.debug(
                f'Order book of symbol with name {symbol_name!r} was updated'
                f' (server timestamp (ms): {server_timestamp_ms})'
            )

            on_new_order_book_data_event(
                action_name=action_name,
                asks=asks,
                bids=bids,
                symbol_name=symbol_name,
                timestamp_ms=server_timestamp_ms,
            )

    async def __subscribe(
        self,
        symbol_name: str,
    ) -> None:
        logger.info(
            f'OKX: Subscribing to {symbol_name!r}...',
        )

        web_socket_connection = self.__web_socket_connection

        assert web_socket_connection is not None, None

        await web_socket_connection.send(
            message=self.__serialize_web_socket_message_raw_data(
                {
                    'op': 'subscribe',
                    'args': [
                        {
                            'channel': 'books',
                            'instId': symbol_name,
                        },
                    ],
                },
            ),
            text=True,
        )

        web_socket_connection_stats_raw_data = (
            self.__web_socket_connection_stats_raw_data
        )

        (web_socket_connection_stats_raw_data['subscriptions_count']) = (
            (web_socket_connection_stats_raw_data['subscriptions_count']) + 1
        )

        logger.info(
            f'OKX: Subscribed to {symbol_name!r}',
        )
