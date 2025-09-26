"""
Модуль для мониторинга и обработки ошибок.
"""

import asyncio
import logging
import traceback
from datetime import UTC, datetime
from typing import Any

from main.process_data.redis_service import g_redis_data_service
from utils.redis import g_redis_manager

logger = logging.getLogger(__name__)


class ErrorHandler:
    """Обработчик ошибок для системы обработки данных."""

    def __init__(self):
        self.error_counts: dict[str, int] = {}
        self.last_errors: dict[str, str] = {}
        self.error_history: list[dict[str, Any]] = []
        self.max_history_size = 1000

    def handle_error(
        self,
        operation: str,
        error: Exception,
        context: dict[str, Any] | None = None,
    ) -> None:
        """Обработка ошибки с логированием и статистикой."""
        error_msg = str(error)
        error_traceback = traceback.format_exc()

        # Обновляем счетчики
        self.error_counts[operation] = self.error_counts.get(operation, 0) + 1
        self.last_errors[operation] = error_msg

        # Добавляем в историю
        error_record = {
            'timestamp': datetime.now(UTC),
            'operation': operation,
            'error': error_msg,
            'traceback': error_traceback,
            'context': context or {},
        }

        self.error_history.append(error_record)

        # Ограничиваем размер истории
        if len(self.error_history) > self.max_history_size:
            self.error_history = self.error_history[-self.max_history_size :]

        # Логируем ошибку
        logger.error(f'Error in {operation}: {error_msg}')
        logger.debug(f'Error traceback: {error_traceback}')

        # Если это критическая ошибка, отправляем уведомление
        if self._is_critical_error(error):
            self._send_critical_error_notification(operation, error, context)

    def _is_critical_error(self, error: Exception) -> bool:
        """Проверка, является ли ошибка критической."""
        critical_errors = [
            'ConnectionError',
            'TimeoutError',
            'MemoryError',
            'RedisError',
        ]

        return any(critical in str(type(error)) for critical in critical_errors)

    def _send_critical_error_notification(
        self,
        operation: str,
        error: Exception,
        context: dict[str, Any] | None,
    ) -> None:
        """Отправка уведомления о критической ошибке."""
        # Здесь должна быть логика отправки уведомлений
        # Например, в Telegram, email, или систему мониторинга
        logger.critical(f'CRITICAL ERROR in {operation}: {error}')

        # Сохраняем критическую ошибку в Redis для мониторинга
        asyncio.create_task(self._save_critical_error(operation, error, context))

    async def _save_critical_error(
        self,
        operation: str,
        error: Exception,
        context: dict[str, Any] | None,
    ) -> None:
        """Сохранение критической ошибки в Redis."""
        try:
            error_data = {
                'timestamp': datetime.now(UTC).isoformat(),
                'operation': operation,
                'error': str(error),
                'context': context or {},
            }

            await g_redis_manager.client.lpush('critical_errors', str(error_data))

            # Ограничиваем количество критических ошибок в Redis
            await g_redis_manager.client.ltrim('critical_errors', 0, 99)

        except Exception as e:
            logger.error(f'Failed to save critical error: {e}')

    def get_error_stats(self) -> dict[str, Any]:
        """Получение статистики ошибок."""
        return {
            'error_counts': self.error_counts.copy(),
            'last_errors': self.last_errors.copy(),
            'total_errors': len(self.error_history),
            'recent_errors': self.error_history[-10:] if self.error_history else [],
        }

    def clear_error_history(self) -> None:
        """Очистка истории ошибок."""
        self.error_counts.clear()
        self.last_errors.clear()
        self.error_history.clear()
        logger.info('Error history cleared')


class SystemMonitor:
    """Монитор системы для отслеживания состояния."""

    def __init__(self):
        self.error_handler = ErrorHandler()
        self.metrics: dict[str, Any] = {}
        self.start_time = datetime.now(UTC)

    async def check_redis_health(self) -> bool:
        """Проверка состояния Redis."""
        try:
            await g_redis_manager.client.ping()

            # Получаем информацию о Redis
            info = await g_redis_manager.client.info()

            # Проверяем использование памяти
            used_memory = info.get('used_memory', 0)
            max_memory = info.get('maxmemory', 0)

            if max_memory > 0 and used_memory > max_memory * 0.9:
                logger.warning(
                    f'Redis memory usage is high: {used_memory}/{max_memory}'
                )

            self.metrics['redis_health'] = {
                'status': 'healthy',
                'used_memory': used_memory,
                'max_memory': max_memory,
                'connected_clients': info.get('connected_clients', 0),
            }

            return True

        except Exception as e:
            self.error_handler.handle_error('redis_health_check', e)
            self.metrics['redis_health'] = {
                'status': 'unhealthy',
                'error': str(e),
            }
            return False

    async def check_data_processing_health(self) -> bool:
        """Проверка состояния обработки данных."""
        try:
            # Проверяем доступность символов
            available_symbols = await g_redis_data_service.load_available_symbols()

            if not available_symbols:
                logger.warning('No available symbols found')
                return False

            # Проверяем статус обработки для нескольких символов
            processing_issues = 0
            for symbol in available_symbols[:5]:  # Проверяем первые 5 символов
                status = await g_redis_data_service.load_processing_status(symbol)

                if status and status.status == 'error':
                    processing_issues += 1
                    logger.warning(
                        f'Processing error for symbol {symbol}: {status.error_message}'
                    )

            self.metrics['data_processing_health'] = {
                'status': 'healthy' if processing_issues == 0 else 'degraded',
                'available_symbols': len(available_symbols),
                'processing_issues': processing_issues,
            }

            return processing_issues == 0

        except Exception as e:
            self.error_handler.handle_error('data_processing_health_check', e)
            self.metrics['data_processing_health'] = {
                'status': 'unhealthy',
                'error': str(e),
            }
            return False

    async def check_system_resources(self) -> bool:
        """Проверка системных ресурсов."""
        try:
            import psutil

            # Проверяем использование CPU
            cpu_percent = psutil.cpu_percent(interval=1)

            # Проверяем использование памяти
            memory = psutil.virtual_memory()

            # Проверяем использование диска
            disk = psutil.disk_usage('/')

            self.metrics['system_resources'] = {
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'memory_available': memory.available,
                'disk_percent': disk.percent,
                'disk_free': disk.free,
            }

            # Проверяем критические пороги
            if cpu_percent > 90:
                logger.warning(f'High CPU usage: {cpu_percent}%')

            if memory.percent > 90:
                logger.warning(f'High memory usage: {memory.percent}%')

            if disk.percent > 90:
                logger.warning(f'High disk usage: {disk.percent}%')

            return cpu_percent < 95 and memory.percent < 95 and disk.percent < 95

        except ImportError:
            logger.warning('psutil not available, skipping system resource check')
            return True
        except Exception as e:
            self.error_handler.handle_error('system_resources_check', e)
            return False

    async def run_health_checks(self) -> dict[str, bool]:
        """Запуск всех проверок здоровья системы."""
        logger.info('Running system health checks...')

        checks = {
            'redis': await self.check_redis_health(),
            'data_processing': await self.check_data_processing_health(),
            'system_resources': await self.check_system_resources(),
        }

        overall_health = all(checks.values())

        logger.info(
            f'Health checks completed. Overall health: {"OK" if overall_health else "DEGRADED"}'
        )

        return checks

    def get_system_metrics(self) -> dict[str, Any]:
        """Получение метрик системы."""
        uptime = datetime.now(UTC) - self.start_time

        return {
            'uptime_seconds': uptime.total_seconds(),
            'uptime_human': str(uptime),
            'metrics': self.metrics.copy(),
            'error_stats': self.error_handler.get_error_stats(),
        }

    async def start_monitoring_loop(self, interval_seconds: int = 300):
        """Запуск цикла мониторинга."""
        logger.info(f'Starting monitoring loop with {interval_seconds}s interval')

        while True:
            try:
                await self.run_health_checks()
                await asyncio.sleep(interval_seconds)
            except Exception as e:
                self.error_handler.handle_error('monitoring_loop', e)
                await asyncio.sleep(60)  # Пауза при ошибке


# Глобальные экземпляры
g_error_handler = ErrorHandler()
g_system_monitor = SystemMonitor()
