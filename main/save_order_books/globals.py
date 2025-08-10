__all__ = (
    'g_globals',
)

from main.save_order_books.okx_web_socket_connection_manager import (
    OKXWebSocketConnectionManager,
)


class Globals(object):
    __slots__ = (
        '__okx_web_socket_connection_manager',
    )

    def __init__(
            self
    ) -> None:
        super().__init__()

        self.__okx_web_socket_connection_manager = (
            OKXWebSocketConnectionManager(
                process_idx=0,
                web_socket_connection_idx=0,
                web_socket_connections_count_per_process=1
            )
        )

    def get_okx_web_socket_connection_manager(
            self,
    ) -> OKXWebSocketConnectionManager:
        return self.__okx_web_socket_connection_manager


g_globals = Globals()
