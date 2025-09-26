"""
Модуль для оптимизации производительности обработки данных.
"""

import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from enumerations.compression import CompressionAlgorithm
from main.process_data.redis_service import g_redis_data_service
from utils.redis import g_redis_manager
from utils.serialization import serialize_dataframe

logger = logging.getLogger(__name__)


class PerformanceOptimizer:
    """Оптимизатор производительности для обработки данных."""

    def __init__(self):
        self.redis_service = g_redis_data_service
        self._cache: dict[str, Any] = {}
        self._cache_timestamps: dict[str, datetime] = {}
        self._cache_ttl = timedelta(minutes=5)  # TTL кэша 5 минут
        self._processing_stats: dict[str, dict[str, Any]] = {}

    async def get_cached_data(
        self, key: str, loader_func: callable, *args, **kwargs
    ) -> Any:
        """Получение данных с кэшированием."""
        now = datetime.now(UTC)

        # Проверяем кэш
        if key in self._cache:
            cache_time = self._cache_timestamps.get(key)
            if cache_time and (now - cache_time) < self._cache_ttl:
                logger.debug(f'Cache hit for key: {key}')
                return self._cache[key]
            else:
                # Удаляем устаревший кэш
                del self._cache[key]
                if key in self._cache_timestamps:
                    del self._cache_timestamps[key]

        # Загружаем данные
        logger.debug(f'Cache miss for key: {key}, loading data')
        start_time = time.time()

        try:
            data = await loader_func(*args, **kwargs)
            load_time = time.time() - start_time

            # Сохраняем в кэш
            self._cache[key] = data
            self._cache_timestamps[key] = now

            # Обновляем статистику
            self._update_stats(key, 'load_time', load_time)
            self._update_stats(key, 'cache_miss', 1)

            logger.debug(f'Data loaded and cached for key: {key} in {load_time:.3f}s')
            return data

        except Exception as e:
            load_time = time.time() - start_time
            logger.error(f'Error loading data for key {key}: {e}')
            self._update_stats(key, 'error_count', 1)
            self._update_stats(key, 'error_time', load_time)
            raise

    def _update_stats(self, key: str, metric: str, value: Any):
        """Обновление статистики производительности."""
        if key not in self._processing_stats:
            self._processing_stats[key] = {}

        if metric not in self._processing_stats[key]:
            self._processing_stats[key][metric] = 0

        if isinstance(value, (int, float)):
            self._processing_stats[key][metric] += value
        else:
            self._processing_stats[key][metric] = value

    async def clear_cache(self, key: str | None = None):
        """Очистка кэша."""
        if key:
            self._cache.pop(key, None)
            self._cache_timestamps.pop(key, None)
            logger.info(f'Cleared cache for key: {key}')
        else:
            self._cache.clear()
            self._cache_timestamps.clear()
            logger.info('Cleared all cache')

    def get_cache_stats(self) -> dict[str, Any]:
        """Получение статистики кэша."""
        return {
            'cache_size': len(self._cache),
            'cache_keys': list(self._cache.keys()),
            'processing_stats': self._processing_stats.copy(),
        }

    async def optimize_redis_connection(self):
        """Оптимизация подключения к Redis."""
        try:
            # Проверяем соединение
            await g_redis_manager.client.ping()

            # Настраиваем pipeline для массовых операций
            pipeline = g_redis_manager.client.pipeline()

            # Получаем информацию о Redis
            info = await g_redis_manager.client.info()

            logger.info(
                f'Redis connection optimized. Memory used: {info.get("used_memory_human", "N/A")}'
            )

        except Exception as e:
            logger.error(f'Error optimizing Redis connection: {e}')

    async def batch_save_data(
        self,
        data_items: list[tuple[str, Any, dict[str, Any]]],
    ) -> bool:
        """Пакетное сохранение данных в Redis."""
        try:
            pipeline = g_redis_manager.client.pipeline()

            for key, data, metadata in data_items:
                # Здесь должна быть логика сериализации и сохранения
                # Упрощенная версия для демонстрации
                pipeline.set(key, str(data))
                pipeline.set(f'{key}:metadata', str(metadata))

            await pipeline.execute()
            logger.info(f'Batch saved {len(data_items)} items to Redis')
            return True

        except Exception as e:
            logger.error(f'Error in batch save: {e}')
            return False

    async def monitor_performance(self):
        """Мониторинг производительности."""
        try:
            # Получаем статистику Redis
            redis_info = await g_redis_manager.client.info()

            # Получаем статистику кэша
            cache_stats = self.get_cache_stats()

            # Логируем статистику
            logger.info(
                f'Performance stats - Cache: {cache_stats["cache_size"]} items, '
                f'Redis memory: {redis_info.get("used_memory_human", "N/A")}'
            )

            # Проверяем производительность
            if cache_stats['cache_size'] > 1000:
                logger.warning('Cache size is large, consider clearing old items')

            return {
                'redis_info': redis_info,
                'cache_stats': cache_stats,
            }

        except Exception as e:
            logger.error(f'Error monitoring performance: {e}')
            return None

    async def cleanup_old_data(self, older_than_hours: int = 24):
        """Очистка старых данных из Redis."""
        try:
            cutoff_time = datetime.now(UTC) - timedelta(hours=older_than_hours)

            # Получаем все ключи
            keys = await g_redis_manager.client.keys('*')

            deleted_count = 0
            for key in keys:
                # Проверяем время последнего обновления
                ttl = await g_redis_manager.client.ttl(key)
                if ttl > 0:  # Ключ имеет TTL
                    continue

                # Проверяем метаданные
                metadata_key = f'{key}:metadata'
                metadata = await g_redis_manager.client.get(metadata_key)

                if metadata:
                    # Здесь должна быть логика проверки времени из метаданных
                    # Упрощенная версия
                    await g_redis_manager.client.delete(key, metadata_key)
                    deleted_count += 1

            logger.info(f'Cleaned up {deleted_count} old data items')
            return deleted_count

        except Exception as e:
            logger.error(f'Error cleaning up old data: {e}')
            return 0

    async def optimize_data_compression(self, symbol_id: str):
        """Оптимизация сжатия данных для символа."""
        try:
            # Получаем данные о сделках
            trades_data = await self.redis_service.load_trades_data(symbol_id)

            if trades_data is None:
                logger.warning(f'No trades data found for {symbol_id}')
                return False

            # Анализируем размер данных
            original_size = len(
                serialize_dataframe(trades_data, compression=CompressionAlgorithm.NONE)
            )
            xz_size = len(
                serialize_dataframe(trades_data, compression=CompressionAlgorithm.XZ)
            )
            lz4_size = len(
                serialize_dataframe(trades_data, compression=CompressionAlgorithm.LZ4)
            )

            compression_ratio_xz = xz_size / original_size if original_size > 0 else 0
            compression_ratio_lz4 = lz4_size / original_size if original_size > 0 else 0

            logger.info(f'Compression analysis for {symbol_id}:')
            logger.info(f'  Original: {original_size:,} bytes')
            logger.info(f'  XZ: {xz_size:,} bytes (ratio: {compression_ratio_xz:.3f})')
            logger.info(
                f'  LZ4: {lz4_size:,} bytes (ratio: {compression_ratio_lz4:.3f})'
            )

            # Выбираем лучший алгоритм сжатия
            if compression_ratio_xz < compression_ratio_lz4:
                logger.info(f'XZ compression is better for {symbol_id}')
                return CompressionAlgorithm.XZ
            else:
                logger.info(f'LZ4 compression is better for {symbol_id}')
                return CompressionAlgorithm.LZ4

        except Exception as e:
            logger.error(f'Error optimizing compression for {symbol_id}: {e}')
            return None


# Глобальный экземпляр оптимизатора
g_performance_optimizer = PerformanceOptimizer()
