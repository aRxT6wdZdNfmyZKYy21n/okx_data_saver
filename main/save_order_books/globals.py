__all__ = (
    'g_globals',
)

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    async_sessionmaker,
    create_async_engine, AsyncSession,
)

from main.save_order_books.okx_web_socket_connection_manager import (
    OKXWebSocketConnectionManager,
)

from settings import (
    settings,
)


class Globals(object):
    __slots__ = (
        '__okx_web_socket_connection_manager',
        '__postgres_db_engine',
        '__postgres_db_session_maker',
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

        self.__postgres_db_engine = postgres_db_engine = create_async_engine(
            'postgresql+asyncpg'
            '://'
            f'{settings.POSTGRES_DB_USER_NAME}'
            ':'
            f'{settings.POSTGRES_DB_PASSWORD.get_secret_value()}'
            '@'
            f'{settings.POSTGRES_DB_HOST_NAME}'
            ':'
            f'{settings.POSTGRES_DB_PORT}'
            '/'
            f'{settings.POSTGRES_DB_NAME}',
            echo=False,  # TODO: enable only for debug mode
        )

        self.__postgres_db_session_maker = async_sessionmaker(
            postgres_db_engine,
            expire_on_commit=False,
        )

    def get_okx_web_socket_connection_manager(
            self,
    ) -> OKXWebSocketConnectionManager:
        return self.__okx_web_socket_connection_manager

    def get_postgres_db_engine(
            self
    ) -> AsyncEngine:
        return self.__postgres_db_engine

    def get_postgres_db_session_maker(
            self
    ) -> async_sessionmaker[AsyncSession]:
        return self.__postgres_db_session_maker


g_globals = Globals()
