"""
Тесты для enum алгоритмов сжатия.
"""

import pytest

from enumerations.compression import CompressionAlgorithm


def test_compression_algorithm_enum():
    """Тест enum алгоритмов сжатия."""
    # Проверяем значения
    assert CompressionAlgorithm.XZ == 'xz'
    assert CompressionAlgorithm.LZ4 == 'lz4'
    assert CompressionAlgorithm.NONE == 'none'

    # Проверяем, что это строки
    assert isinstance(CompressionAlgorithm.XZ, str)
    assert isinstance(CompressionAlgorithm.LZ4, str)
    assert isinstance(CompressionAlgorithm.NONE, str)

    # Проверяем итерацию
    algorithms = list(CompressionAlgorithm)
    assert len(algorithms) == 3
    assert CompressionAlgorithm.XZ in algorithms
    assert CompressionAlgorithm.LZ4 in algorithms
    assert CompressionAlgorithm.NONE in algorithms


def test_compression_algorithm_comparison():
    """Тест сравнения enum значений."""
    # Прямое сравнение
    assert CompressionAlgorithm.XZ == 'xz'
    assert CompressionAlgorithm.LZ4 == 'lz4'
    assert CompressionAlgorithm.NONE == 'none'

    # Сравнение с другими значениями
    assert CompressionAlgorithm.XZ != 'lz4'
    assert CompressionAlgorithm.LZ4 != 'xz'
    assert CompressionAlgorithm.NONE != 'xz'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
