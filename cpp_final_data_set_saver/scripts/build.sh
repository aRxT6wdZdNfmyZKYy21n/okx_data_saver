#!/bin/bash

# Скрипт сборки OKX Final Data Set Saver

set -e

echo "Building OKX Final Data Set Saver..."

# Создаем директорию сборки
mkdir -p build
cd build

# Конфигурируем CMake
echo "Configuring CMake..."
cmake .. -DCMAKE_BUILD_TYPE=Release

# Собираем проект
echo "Building project..."
make -j$(nproc)

echo "Build completed successfully!"
echo "Executable: build/okx_final_data_set_saver"
