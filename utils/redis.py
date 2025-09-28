"""
Утилиты для работы с Redis.
"""

import json
import logging
import traceback
import typing
from typing import Any

import polars
import redis.asyncio as redis
from redis.asyncio import Redis

from enumerations import CompressionAlgorithm
from settings import settings
from utils.serialization import (
    deserialize_dataframe,
    estimate_dataframe_size,
    get_compression_ratio,
    merge_compressed_data_chunks,
    serialize_dataframe,
    split_compressed_data_by_size,
)

logger = logging.getLogger(
    __name__,
)


class RedisManager:
    """Менеджер для работы с Redis."""

    def __init__(
        self,
    ) -> None:
        super().__init__()

        self.__redis: Redis | None = None

    async def connect(
        self,
    ) -> None:
        """Установка соединения с Redis."""
        try:
            self.__redis = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                password=settings.REDIS_PASSWORD.get_secret_value()
                if settings.REDIS_PASSWORD
                else None,
                db=settings.REDIS_DB,
                decode_responses=False,  # Работаем с бинарными данными
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
            )

            # Проверка соединения
            await self.__redis.ping()

            logger.info('Connected to Redis successfully')
        except Exception as exception:
            logger.error(
                'Failed to connect to Redis'
                f': {"".join(traceback.format_exception(exception))}',
            )

            raise exception

    async def disconnect(self) -> None:
        """Закрытие соединения с Redis."""
        if self.__redis:
            await self.__redis.close()
            logger.info('Disconnected from Redis')

    async def get(
        self,
        key: str,
    ) -> bytes:
        redis_ = self.__redis
        if not redis_:
            raise RuntimeError('Redis not connected')

        return await redis_.get(
            key,
        )

    async def get_info(
        self,
    ) -> dict[str, typing.Any]:
        redis_ = self.__redis
        if not redis_:
            raise RuntimeError('Redis not connected')

        return await redis_.info()

    async def save_dataframe(
        self,
        key: str,
        dataframe: polars.DataFrame,
        compression: CompressionAlgorithm,
        max_size_bytes: int,
    ) -> dict[str, Any]:
        """
        Сохранение DataFrame в Redis с разбивкой на части при необходимости.

        Новый алгоритм:
        1) Сжимаем DataFrame один раз
        2) Разбиваем сжатые данные на чанки при необходимости
        3) Сохраняем чанки

        Args:
            key: Ключ Redis
            dataframe: Polars DataFrame
            compression: Алгоритм сжатия
            max_size_bytes: Максимальный размер части

        Returns:
            Метаданные о сохранении
        """
        redis_ = self.__redis
        if not redis_:
            raise RuntimeError('Redis not connected')

        # Шаг 1: Сжимаем DataFrame один раз
        logger.info(f'[save_dataframe][{key}]: Compressing DataFrame...')

        compressed_data = serialize_dataframe(
            dataframe,
            compression,
        )

        logger.info(
            f'[save_dataframe][{key}]: compressed_size: {len(compressed_data)}B',
        )

        # Шаг 2: Разбиваем сжатые данные на чанки при необходимости
        data_chunks = split_compressed_data_by_size(
            compressed_data,
            max_size_bytes,
        )

        parts_count = len(
            data_chunks,
        )

        logger.info(
            f'[save_dataframe][{key}]: Split into {parts_count} chunks',
        )

        # Шаг 3: Сохраняем чанки
        if parts_count == 1:
            # Сохраняем как единое целое
            await redis_.set(key, compressed_data)
        else:
            # Сохраняем каждый чанк
            for i, chunk in enumerate(data_chunks):
                chunk_key = f'{key}:part_{i}'
                await redis_.set(chunk_key, chunk)

        # Вычисляем метаданные
        original_size = estimate_dataframe_size(
            dataframe,
            CompressionAlgorithm.NONE,
        )

        metadata = {
            'compression': str(compression.value),
            'compression_ratio': get_compression_ratio(
                original_size,
                len(compressed_data),
            ),
            'parts_count': parts_count,
            'total_size': len(compressed_data),
        }

        # Сохраняем метаданные
        await redis_.set(
            f'{key}:metadata',
            json.dumps(metadata),
        )

        logger.info(
            f'Saved DataFrame to Redis: {key}, size: {metadata["total_size"]} bytes, parts: {parts_count}',
        )

        return metadata

    async def load_dataframe(
        self,
        key: str,
    ) -> Any | None:  # Optional[polars.DataFrame]
        """
        Загрузка DataFrame из Redis.

        Новый алгоритм:
        1) Загружаем чанки сжатых данных
        2) Склеиваем чанки в единый блок
        3) Разжимаем DataFrame

        Args:
            key: Ключ Redis

        Returns:
            Polars DataFrame или None если не найден
        """
        redis_ = self.__redis
        if not redis_:
            raise RuntimeError('Redis not connected')

        # Загружаем метаданные
        metadata_raw = await redis_.get(f'{key}:metadata')
        if not metadata_raw:
            return None

        metadata = json.loads(metadata_raw)
        compression_raw = metadata['compression']
        compression = CompressionAlgorithm(compression_raw)
        parts_count = metadata['parts_count']

        # Шаг 1: Загружаем чанки сжатых данных
        compressed_chunks = []

        if parts_count == 1:
            # Загружаем как единое целое
            data = await redis_.get(key)
            if not data:
                return None
            compressed_chunks = [data]
        else:
            # Загружаем все части
            for i in range(parts_count):
                chunk_key = f'{key}:part_{i}'
                chunk_data = await redis_.get(chunk_key)

                if not chunk_data:
                    logger.warning(f'Missing chunk {i} for key {key}')
                    return None

                compressed_chunks.append(chunk_data)

        if not compressed_chunks:
            return None

        # Шаг 2: Склеиваем чанки в единый блок
        logger.info(
            f'[load_dataframe][{key}]: Merging {len(compressed_chunks)} chunks...'
        )
        compressed_data = merge_compressed_data_chunks(compressed_chunks)

        # Шаг 3: Разжимаем DataFrame
        logger.info(f'[load_dataframe][{key}]: Decompressing DataFrame...')
        return deserialize_dataframe(compressed_data, compression)

    async def delete_dataframe(
        self,
        key: str,
    ) -> None:
        """
        Удаление DataFrame из Redis.

        Args:
            key: Ключ Redis
        """
        redis_ = self.__redis
        if not redis_:
            raise RuntimeError('Redis not connected')

        # Загружаем метаданные для определения количества частей
        metadata_raw = await redis_.get(
            f'{key}:metadata',
        )

        if metadata_raw is not None:
            metadata = json.loads(metadata_raw)
            parts_count = metadata.get('parts_count', 1)

            # Удаляем все части
            keys_to_delete = [key, f'{key}:metadata']
            for i in range(parts_count):
                keys_to_delete.append(f'{key}:part_{i}')

            await self.__redis.delete(*keys_to_delete)
            logger.info(f'Deleted DataFrame from Redis: {key}')

    async def exists(self, key: str) -> bool:
        """
        Проверка существования ключа в Redis.

        Args:
            key: Ключ Redis

        Returns:
            True если ключ существует
        """
        if not self.__redis:
            raise RuntimeError('Redis not connected')

        return (
            await self.__redis.exists(
                key,
            )
            > 0
        )

    async def get_metadata(self, key: str) -> dict[str, Any] | None:
        """
        Получение метаданных DataFrame.

        Args:
            key: Ключ Redis

        Returns:
            Метаданные или None если не найдены
        """
        redis_ = self.__redis
        if not redis_:
            raise RuntimeError('Redis not connected')

        metadata_raw = await redis_.get(
            f'{key}:metadata',
        )

        if not metadata_raw:
            return None

        return json.loads(
            metadata_raw,
        )

    async def ping(self) -> None:
        redis_ = self.__redis
        if not redis_:
            raise RuntimeError('Redis not connected')

        await redis_.ping()

    async def set(
        self,
        key: str,
        value: str,
    ) -> None:
        redis_ = self.__redis
        if not redis_:
            raise RuntimeError('Redis not connected')

        await redis_.set(
            key,
            value,
        )

    async def set_metadata(
        self,
        key: str,
        metadata_raw_data_raw: str,
    ) -> None:
        """
        Запись метаданных DataFrame.

        Args:
            key: Ключ Redis
            metadata_raw_data_raw: JSON-представление метаданных
        Returns:
            Метаданные или None если не найдены
        """

        await self.set(
            f'{key}:metadata',
            metadata_raw_data_raw,
        )


# Глобальный экземпляр менеджера Redis
g_redis_manager = RedisManager()
