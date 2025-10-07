#!/bin/bash

# Скрипт запуска OKX Final Data Set Saver

set -e

echo "Starting OKX Final Data Set Saver..."

# Проверяем, что исполняемый файл существует
if [ ! -f "build/okx_final_data_set_saver" ]; then
    echo "Error: Executable not found. Please run build.sh first."
    exit 1
fi

# Проверяем наличие .env файла
if [ ! -f ".env" ] && [ ! -f "config/.env" ]; then
    echo "Warning: No .env file found. Please create one based on config/env_example.txt"
    echo "You can copy the example: cp config/env_example.txt .env"
    echo "Then edit .env with your database credentials."
    exit 1
fi

# Запускаем приложение
echo "Starting application..."
./build/okx_final_data_set_saver
