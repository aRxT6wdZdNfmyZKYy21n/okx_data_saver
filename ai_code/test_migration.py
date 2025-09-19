#!/usr/bin/env python3
"""
Простой тест для проверки корректности скрипта миграции.
Проверяет синтаксис и импорты.
"""

import sys
import os


def test_imports():
    """Тест импортов."""
    try:
        from enumerations import SymbolId

        print('✓ Импорт SymbolId успешен')

        # Проверяем значения
        assert SymbolId.BTC_USDT.value == 1
        assert SymbolId.ETH_USDT.value == 2
        assert SymbolId.SOL_USDT.value == 3
        print('✓ Значения SymbolId корректны')

    except ImportError as e:
        print(f'✗ Ошибка импорта: {e}')
        return False
    except AssertionError as e:
        print(f'✗ Ошибка значений SymbolId: {e}')
        return False

    return True


def test_migration_script_syntax():
    """Тест синтаксиса скрипта миграции."""
    try:
        # Проверяем только синтаксис, не импортируем модуль полностью
        import ast
        import os

        # Получаем путь к файлу относительно текущего модуля
        current_dir = os.path.dirname(__file__)
        script_path = os.path.join(current_dir, 'migrate_symbol_name_to_symbol_id.py')

        with open(script_path, 'r', encoding='utf-8') as f:
            source = f.read()

        # Парсим код для проверки синтаксиса
        ast.parse(source)
        print('✓ Синтаксис скрипта миграции корректен')
        return True
    except SyntaxError as e:
        print(f'✗ Синтаксическая ошибка в скрипте миграции: {e}')
        return False
    except Exception as e:
        print(f'✗ Ошибка при проверке скрипта миграции: {e}')
        return False


def test_settings_import():
    """Тест импорта настроек."""
    try:
        # Проверяем, что модуль settings импортируется
        import settings

        print('✓ Импорт модуля settings успешен')

        # Проверяем, что класс Settings существует
        if hasattr(settings, 'Settings'):
            print('✓ Класс Settings найден')
        else:
            print('✗ Класс Settings не найден')
            return False

        # Проверяем, что все необходимые поля определены в классе
        required_fields = [
            'POSTGRES_DB_HOST_NAME',
            'POSTGRES_DB_NAME',
            'POSTGRES_DB_PORT',
            'POSTGRES_DB_PASSWORD',
            'POSTGRES_DB_USER_NAME',
        ]

        settings_class = settings.Settings
        for field in required_fields:
            if not hasattr(settings_class, field):
                print(f'✗ Отсутствует поле настроек: {field}')
                return False

        print('✓ Все необходимые поля настроек определены в классе')
        print(
            'ℹ Примечание: Для полного тестирования создайте файл .env с реальными настройками'
        )
        return True

    except ImportError as e:
        print(f'✗ Ошибка импорта модуля settings: {e}')
        return False
    except Exception as e:
        # Игнорируем ошибки валидации, так как .env файл может отсутствовать
        if 'validation errors' in str(e) or 'Field required' in str(e):
            print(
                '✓ Структура настроек корректна (ошибка валидации ожидаема без .env файла)'
            )
            return True
        print(f'✗ Ошибка при проверке настроек: {e}')
        return False


def test_schemas():
    """Тест схем."""
    try:
        from main.save_trades.schemas import OKXTradeData2
        from main.save_order_books.schemas import OKXOrderBookData2
        from main.save_candles.schemas import OKXCandleData15m2, OKXCandleData1H2

        print('✓ Импорт новых схем успешен')

        # Проверяем имена таблиц
        assert OKXTradeData2.__tablename__ == 'okx_trade_data_2'
        assert OKXOrderBookData2.__tablename__ == 'okx_order_book_data_2'
        assert OKXCandleData15m2.__tablename__ == 'okx_candle_data_15m_2'
        assert OKXCandleData1H2.__tablename__ == 'okx_candle_data_1H_2'
        print('✓ Имена таблиц корректны')

        return True
    except ImportError as e:
        print(f'✗ Ошибка импорта схем: {e}')
        return False
    except AssertionError as e:
        print(f'✗ Ошибка имен таблиц: {e}')
        return False


def main():
    """Главная функция тестирования."""
    print('Запуск тестов миграции...')
    print('=' * 50)

    tests = [
        test_imports,
        test_migration_script_syntax,
        test_settings_import,
        test_schemas,
    ]

    passed = 0
    total = len(tests)

    for test in tests:
        if test():
            passed += 1
        print()

    print('=' * 50)
    print(f'Результат: {passed}/{total} тестов пройдено')

    if passed == total:
        print('✓ Все тесты пройдены успешно!')
        return 0
    else:
        print('✗ Некоторые тесты не пройдены')
        return 1


if __name__ == '__main__':
    sys.exit(main())
