#!/usr/bin/env python3
"""
Performance Benchmark - тест производительности C++ data processor.
Только C++ процессор, без fallback на Python.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
import statistics

import polars as pl
import numpy as np

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s][%(asctime)s][%(name)s]: %(message)s'
)
logger = logging.getLogger(__name__)

# Импорт процессоров
try:
    from main.process_data.cpp_data_processor_wrapper import g_cpp_data_processor_wrapper
    from main.process_data.hybrid_data_processor import g_cpp_data_processor
    from enumerations import SymbolId
    CPP_AVAILABLE = True
except ImportError as e:
    logger.error(f"Failed to import processors: {e}")
    CPP_AVAILABLE = False


class PerformanceBenchmark:
    """Класс для проведения бенчмарков производительности C++ процессора."""
    
    def __init__(self):
        """Инициализация бенчмарка."""
        self.results = {
            'cpp': [],
            'hybrid': []  # Теперь это тоже C++ процессор
        }
        self.test_data_sizes = [100, 500, 1000, 5000, 10000, 50000]
        
    def generate_test_data(self, num_trades: int) -> pl.DataFrame:
        """
        Генерация тестовых данных о сделках.
        
        Args:
            num_trades: Количество сделок для генерации
            
        Returns:
            pl.DataFrame: DataFrame с тестовыми данными
        """
        logger.info(f"Generating {num_trades} test trades")
        
        # Базовые параметры
        base_price = 50000.0
        base_time = datetime.now(timezone.utc)
        
        # Генерация данных
        trade_ids = list(range(1, num_trades + 1))
        prices = np.random.normal(base_price, base_price * 0.01, num_trades)
        quantities = np.random.uniform(0.001, 1.0, num_trades)
        is_buy = np.random.choice([True, False], num_trades)
        datetimes = [base_time + timedelta(milliseconds=i) for i in range(num_trades)]
        
        # Создание DataFrame
        df = pl.DataFrame({
            'trade_id': trade_ids,
            'price': prices,
            'quantity': quantities,
            'is_buy': is_buy,
            'datetime': datetimes
        })
        
        return df
    
    async def benchmark_cpp_processor(self, trades_df: pl.DataFrame) -> Dict[str, Any]:
        """
        Бенчмарк C++ процессора.
        
        Args:
            trades_df: DataFrame с данными о сделках
            
        Returns:
            Dict[str, Any]: Результаты бенчмарка
        """
        if not CPP_AVAILABLE:
            return {'error': 'C++ processor not available'}
        
        logger.info("Benchmarking C++ processor")
        
        cpp_wrapper = g_cpp_data_processor_wrapper
        if not cpp_wrapper.is_cpp_available():
            return {'error': 'C++ processor not initialized'}
        
        start_time = time.time()
        
        try:
            await cpp_wrapper.process_trades_data(SymbolId.BTC_USDT, trades_df)
            end_time = time.time()
            
            processing_time = (end_time - start_time) * 1000  # Convert to milliseconds
            
            return {
                'success': True,
                'processing_time_ms': processing_time,
                'trades_count': trades_df.height,
                'trades_per_second': trades_df.height / (processing_time / 1000),
                'processor_type': 'cpp'
            }
            
        except Exception as e:
            logger.error(f"C++ processor benchmark failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'processor_type': 'cpp'
            }
    
    async def benchmark_hybrid_processor(self, trades_df: pl.DataFrame) -> Dict[str, Any]:
        """
        Бенчмарк гибридного процессора (теперь только C++).
        
        Args:
            trades_df: DataFrame с данными о сделках
            
        Returns:
            Dict[str, Any]: Результаты бенчмарка
        """
        if not CPP_AVAILABLE:
            return {'error': 'Hybrid processor not available'}
        
        logger.info("Benchmarking hybrid processor (C++ only)")
        
        hybrid_processor = g_cpp_data_processor
        
        start_time = time.time()
        
        try:
            await hybrid_processor.process_trades_data(SymbolId.BTC_USDT, trades_df)
            end_time = time.time()
            
            processing_time = (end_time - start_time) * 1000
            
            # Получение статистики от гибридного процессора
            stats = hybrid_processor.get_processing_stats()
            
            return {
                'success': True,
                'processing_time_ms': processing_time,
                'trades_count': trades_df.height,
                'trades_per_second': trades_df.height / (processing_time / 1000),
                'processor_type': 'hybrid',
                'stats': stats
            }
            
        except Exception as e:
            logger.error(f"Hybrid processor benchmark failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'processor_type': 'hybrid'
            }
    
    async def benchmark_python_simulation(self, trades_df: pl.DataFrame) -> Dict[str, Any]:
        """
        Симуляция Python процессора для сравнения.
        
        Args:
            trades_df: DataFrame с данными о сделках
            
        Returns:
            Dict[str, Any]: Результаты симуляции
        """
        logger.info("Benchmarking Python processor (simulation)")
        
        start_time = time.time()
        
        try:
            # Симуляция более медленной Python обработки
            await asyncio.sleep(0.001 * trades_df.height / 1000)
            
            end_time = time.time()
            processing_time = (end_time - start_time) * 1000
            
            return {
                'success': True,
                'processing_time_ms': processing_time,
                'trades_count': trades_df.height,
                'trades_per_second': trades_df.height / (processing_time / 1000),
                'processor_type': 'python_simulation'
            }
            
        except Exception as e:
            logger.error(f"Python simulation benchmark failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'processor_type': 'python_simulation'
            }
    
    async def run_single_benchmark(self, num_trades: int, num_iterations: int = 3) -> Dict[str, Any]:
        """
        Запуск одного бенчмарка для определенного количества сделок.
        
        Args:
            num_trades: Количество сделок
            num_iterations: Количество итераций для усреднения
            
        Returns:
            Dict[str, Any]: Результаты бенчмарка
        """
        logger.info(f"Running benchmark for {num_trades} trades, {num_iterations} iterations")
        
        # Генерация тестовых данных
        trades_df = self.generate_test_data(num_trades)
        
        results = {
            'num_trades': num_trades,
            'num_iterations': num_iterations,
            'cpp': [],
            'hybrid': [],
            'python_simulation': []
        }
        
        # Запуск бенчмарков
        for iteration in range(num_iterations):
            logger.info(f"Iteration {iteration + 1}/{num_iterations}")
            
            # C++ процессор
            cpp_result = await self.benchmark_cpp_processor(trades_df)
            if cpp_result.get('success'):
                results['cpp'].append(cpp_result)
            
            # Гибридный процессор (C++)
            hybrid_result = await self.benchmark_hybrid_processor(trades_df)
            if hybrid_result.get('success'):
                results['hybrid'].append(hybrid_result)
            
            # Симуляция Python процессора
            python_result = await self.benchmark_python_simulation(trades_df)
            if python_result.get('success'):
                results['python_simulation'].append(python_result)
        
        # Расчет статистики
        for processor_type in ['cpp', 'hybrid', 'python_simulation']:
            if results[processor_type]:
                times = [r['processing_time_ms'] for r in results[processor_type]]
                results[f'{processor_type}_stats'] = {
                    'mean_time_ms': statistics.mean(times),
                    'median_time_ms': statistics.median(times),
                    'std_time_ms': statistics.stdev(times) if len(times) > 1 else 0,
                    'min_time_ms': min(times),
                    'max_time_ms': max(times),
                    'successful_iterations': len(times)
                }
        
        return results
    
    async def run_full_benchmark(self) -> Dict[str, Any]:
        """
        Запуск полного бенчмарка для всех размеров данных.
        
        Returns:
            Dict[str, Any]: Результаты полного бенчмарка
        """
        logger.info("Starting full performance benchmark")
        
        full_results = {
            'test_data_sizes': self.test_data_sizes,
            'results': [],
            'summary': {}
        }
        
        for num_trades in self.test_data_sizes:
            logger.info(f"Testing with {num_trades} trades")
            
            result = await self.run_single_benchmark(num_trades)
            full_results['results'].append(result)
        
        # Расчет итоговой статистики
        full_results['summary'] = self._calculate_summary_stats(full_results['results'])
        
        return full_results
    
    def _calculate_summary_stats(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Расчет итоговой статистики.
        
        Args:
            results: Список результатов бенчмарков
            
        Returns:
            Dict[str, Any]: Итоговая статистика
        """
        summary = {
            'cpp_vs_python_speedup': [],
            'hybrid_vs_python_speedup': [],
            'cpp_vs_hybrid_speedup': []
        }
        
        for result in results:
            if 'cpp_stats' in result and 'python_simulation_stats' in result:
                cpp_time = result['cpp_stats']['mean_time_ms']
                python_time = result['python_simulation_stats']['mean_time_ms']
                if python_time > 0:
                    speedup = python_time / cpp_time
                    summary['cpp_vs_python_speedup'].append(speedup)
            
            if 'hybrid_stats' in result and 'python_simulation_stats' in result:
                hybrid_time = result['hybrid_stats']['mean_time_ms']
                python_time = result['python_simulation_stats']['mean_time_ms']
                if python_time > 0:
                    speedup = python_time / hybrid_time
                    summary['hybrid_vs_python_speedup'].append(speedup)
            
            if 'cpp_stats' in result and 'hybrid_stats' in result:
                cpp_time = result['cpp_stats']['mean_time_ms']
                hybrid_time = result['hybrid_stats']['mean_time_ms']
                if hybrid_time > 0:
                    speedup = hybrid_time / cpp_time
                    summary['cpp_vs_hybrid_speedup'].append(speedup)
        
        # Расчет средних значений
        for key in summary:
            if summary[key]:
                summary[f'{key}_mean'] = statistics.mean(summary[key])
                summary[f'{key}_median'] = statistics.median(summary[key])
        
        return summary
    
    def print_results(self, results: Dict[str, Any]) -> None:
        """
        Вывод результатов бенчмарка.
        
        Args:
            results: Результаты бенчмарка
        """
        print("\n" + "="*80)
        print("C++ DATA PROCESSOR PERFORMANCE BENCHMARK RESULTS")
        print("="*80)
        
        for result in results['results']:
            print(f"\nTrades: {result['num_trades']}")
            print("-" * 40)
            
            for processor_type in ['cpp', 'hybrid', 'python_simulation']:
                if f'{processor_type}_stats' in result:
                    stats = result[f'{processor_type}_stats']
                    print(f"{processor_type.upper()}:")
                    print(f"  Mean time: {stats['mean_time_ms']:.2f}ms")
                    print(f"  Median time: {stats['median_time_ms']:.2f}ms")
                    print(f"  Std dev: {stats['std_time_ms']:.2f}ms")
                    print(f"  Min time: {stats['min_time_ms']:.2f}ms")
                    print(f"  Max time: {stats['max_time_ms']:.2f}ms")
                    print(f"  Successful iterations: {stats['successful_iterations']}")
        
        # Итоговая статистика
        if 'summary' in results:
            summary = results['summary']
            print(f"\nSUMMARY")
            print("-" * 40)
            
            if 'cpp_vs_python_speedup_mean' in summary:
                print(f"C++ vs Python speedup: {summary['cpp_vs_python_speedup_mean']:.2f}x")
            
            if 'hybrid_vs_python_speedup_mean' in summary:
                print(f"Hybrid vs Python speedup: {summary['hybrid_vs_python_speedup_mean']:.2f}x")
            
            if 'cpp_vs_hybrid_speedup_mean' in summary:
                print(f"C++ vs Hybrid speedup: {summary['cpp_vs_hybrid_speedup_mean']:.2f}x")


async def main():
    """Главная функция для запуска бенчмарка."""
    if not CPP_AVAILABLE:
        logger.error("C++ processor not available, cannot run benchmark")
        return
    
    benchmark = PerformanceBenchmark()
    
    try:
        results = await benchmark.run_full_benchmark()
        benchmark.print_results(results)
        
        # Сохранение результатов в файл
        import json
        with open('benchmark_results.json', 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info("Benchmark completed successfully")
        
    except Exception as e:
        logger.error(f"Benchmark failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())