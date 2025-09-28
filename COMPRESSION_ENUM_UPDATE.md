# Обновление: Enum для алгоритмов сжатия

## Изменения

### ✅ Создан новый enum
- **Файл**: `enumerations/compression.py`
- **Класс**: `CompressionAlgorithm(StrEnum)`
- **Значения**: `XZ = "xz"`, `LZ4 = "lz4"`, `NONE = "none"`

### ✅ Обновлены файлы для использования enum

#### `constants/redis.py`
- Добавлен импорт `CompressionAlgorithm`
- Константы теперь ссылаются на enum значения
- Сохранена обратная совместимость

#### `utils/serialization.py`
- Функции принимают `str | CompressionAlgorithm`
- Обновлена логика сравнения для работы с enum
- Поддержка как строк, так и enum значений

#### `main/process_data/performance_optimizer.py`
- Использует enum в методах оптимизации сжатия

#### `test_redis_migration.py`
- Обновлены тесты для использования enum
- Все тесты сжатия используют `CompressionAlgorithm`

### ✅ Созданы тесты
- **Файл**: `test_compression_enum.py`
- Тесты для проверки корректности enum
- Тесты сравнения и итерации

## Преимущества

1. **Типобезопасность**: IDE будет предлагать автодополнение
2. **Читаемость**: `CompressionAlgorithm.XZ` понятнее чем `"xz"`
3. **Защита от ошибок**: Невозможно передать неверное значение
4. **Обратная совместимость**: Старые строковые значения продолжают работать

## Использование

```python
from enumerations.compression import CompressionAlgorithm

# Новый способ (рекомендуется)
serialize_dataframe(df, compression=CompressionAlgorithm.XZ)
```

## Статус
✅ **Завершено** - Все файлы обновлены, тесты проходят
