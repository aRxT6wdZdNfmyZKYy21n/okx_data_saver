"""
Configuration for C++ Data Processor integration.
Only C++ processor, no Python fallback.
"""

from typing import Dict, Any, List
from dataclasses import dataclass
from enum import Enum


class ProcessorMode(Enum):
    """Режимы работы процессора."""
    CPP_ONLY = "cpp_only"
    AUTO = "auto"  # Автоматический выбор на основе размера данных


@dataclass
class CppProcessorConfig:
    """Конфигурация C++ процессора."""
    
    # Основные настройки
    mode: ProcessorMode = ProcessorMode.CPP_ONLY
    enable_cpp: bool = True
    
    # Настройки производительности
    prefer_cpp_for_large_datasets: bool = True
    large_dataset_threshold: int = 1000  # Количество сделок
    
    # Настройки обработки
    enable_bollinger_bands: bool = True
    enable_candles: bool = True
    enable_rsi: bool = True
    enable_smoothing: bool = True
    enable_extreme_lines: bool = True
    enable_order_book_volumes: bool = True
    enable_velocity: bool = True
    
    # Параметры Bollinger Bands
    bollinger_period: int = 20
    bollinger_std_dev_multiplier: float = 2.0
    
    # Параметры RSI
    rsi_period: int = 14
    
    # Интервалы свечей
    candle_intervals: List[str] = None
    
    # Уровни сглаживания
    smoothing_levels: List[str] = None
    
    # Настройки мониторинга
    enable_performance_monitoring: bool = True
    log_processing_times: bool = True
    collect_detailed_stats: bool = True
    
    # Настройки Redis
    enable_redis_caching: bool = True
    redis_cache_ttl: int = 3600  # В секундах
    
    def __post_init__(self):
        """Инициализация значений по умолчанию."""
        if self.candle_intervals is None:
            self.candle_intervals = ['1m', '5m', '15m', '1h', '4h', '1d']
        
        if self.smoothing_levels is None:
            self.smoothing_levels = ['Raw (0)', 'Smoothed (1)']

    def to_cpp_params(self) -> Dict[str, Any]:
        """
        Конвертация в параметры для C++ процессора.
        
        Returns:
            Dict[str, Any]: Параметры для C++ процессора
        """
        return {
            'enable_bollinger_bands': self.enable_bollinger_bands,
            'enable_candles': self.enable_candles,
            'enable_rsi': self.enable_rsi,
            'enable_smoothing': self.enable_smoothing,
            'enable_extreme_lines': self.enable_extreme_lines,
            'enable_order_book_volumes': self.enable_order_book_volumes,
            'enable_velocity': self.enable_velocity,
            'bollinger_period': self.bollinger_period,
            'bollinger_std_dev_multiplier': self.bollinger_std_dev_multiplier,
            'rsi_period': self.rsi_period,
            'candle_intervals': self.candle_intervals,
            'smoothing_levels': self.smoothing_levels
        }

    def should_use_cpp(self, trades_count: int) -> bool:
        """
        Определение, следует ли использовать C++ процессор.
        Всегда возвращает True, так как только C++ процессор доступен.
        
        Args:
            trades_count: Количество сделок для обработки
            
        Returns:
            bool: Всегда True (только C++ процессор)
        """
        if not self.enable_cpp:
            raise RuntimeError("C++ processor is disabled but required")
        
        if self.mode == ProcessorMode.CPP_ONLY:
            return True
        
        if self.mode == ProcessorMode.AUTO:
            # Автоматический выбор на основе размера данных
            return trades_count >= self.large_dataset_threshold
        
        return True


# Глобальная конфигурация
DEFAULT_CONFIG = CppProcessorConfig()

# Конфигурации для разных сценариев
CONFIGS = {
    'development': CppProcessorConfig(
        mode=ProcessorMode.CPP_ONLY,
        enable_performance_monitoring=True,
        log_processing_times=True,
        collect_detailed_stats=True
    ),
    
    'production': CppProcessorConfig(
        mode=ProcessorMode.CPP_ONLY,
        enable_performance_monitoring=True,
        log_processing_times=False,
        collect_detailed_stats=False,
        prefer_cpp_for_large_datasets=True,
        large_dataset_threshold=500
    ),
    
    'testing': CppProcessorConfig(
        mode=ProcessorMode.CPP_ONLY,
        enable_performance_monitoring=True,
        log_processing_times=True,
        collect_detailed_stats=True
    ),
    
    'benchmark': CppProcessorConfig(
        mode=ProcessorMode.AUTO,
        enable_performance_monitoring=True,
        log_processing_times=True,
        collect_detailed_stats=True,
        prefer_cpp_for_large_datasets=False
    )
}


def get_config(profile: str = 'development') -> CppProcessorConfig:
    """
    Получение конфигурации по профилю.
    
    Args:
        profile: Профиль конфигурации
        
    Returns:
        CppProcessorConfig: Конфигурация процессора
    """
    return CONFIGS.get(profile, DEFAULT_CONFIG)


def update_config(config: CppProcessorConfig, **kwargs) -> CppProcessorConfig:
    """
    Обновление конфигурации.
    
    Args:
        config: Исходная конфигурация
        **kwargs: Новые значения параметров
        
    Returns:
        CppProcessorConfig: Обновленная конфигурация
    """
    for key, value in kwargs.items():
        if hasattr(config, key):
            setattr(config, key, value)
    
    return config


# Настройки для разных символов
SYMBOL_CONFIGS = {
    'BTC_USDT': CppProcessorConfig(
        mode=ProcessorMode.CPP_ONLY,
        large_dataset_threshold=1000,
        enable_bollinger_bands=True,
        enable_candles=True,
        enable_rsi=True,
        enable_smoothing=True,
        enable_extreme_lines=True,
        enable_order_book_volumes=True,
        enable_velocity=True
    ),
    
    'ETH_USDT': CppProcessorConfig(
        mode=ProcessorMode.CPP_ONLY,
        large_dataset_threshold=500,
        enable_bollinger_bands=True,
        enable_candles=True,
        enable_rsi=True,
        enable_smoothing=True,
        enable_extreme_lines=True,
        enable_order_book_volumes=True,
        enable_velocity=True
    ),
    
    'default': DEFAULT_CONFIG
}


def get_symbol_config(symbol: str) -> CppProcessorConfig:
    """
    Получение конфигурации для конкретного символа.
    
    Args:
        symbol: Название символа
        
    Returns:
        CppProcessorConfig: Конфигурация для символа
    """
    return SYMBOL_CONFIGS.get(symbol, SYMBOL_CONFIGS['default'])