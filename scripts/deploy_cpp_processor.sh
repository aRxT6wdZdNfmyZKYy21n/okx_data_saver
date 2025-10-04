#!/bin/bash
#
# Deployment Script - скрипт для развертывания и обновления C++ data processor.
#

set -euo pipefail

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Настройки по умолчанию
PROJECT_ROOT="."
SKIP_TESTS=false
BUILD_DIR=""
TARGET_DIR=""

# Функция логирования
log() {
    local level="$1"
    shift
    local message="$*"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    case "$level" in
        "INFO")
            echo -e "${BLUE}[INFO][${timestamp}]: ${message}${NC}"
            ;;
        "ERROR")
            echo -e "${RED}[ERROR][${timestamp}]: ${message}${NC}"
            ;;
        "WARNING")
            echo -e "${YELLOW}[WARNING][${timestamp}]: ${message}${NC}"
            ;;
        "SUCCESS")
            echo -e "${GREEN}[SUCCESS][${timestamp}]: ${message}${NC}"
            ;;
    esac
}

# Функция для отображения справки
show_help() {
    cat << EOF
Deploy C++ Data Processor

Usage: $0 [OPTIONS]

Options:
    --skip-tests          Skip tests during deployment
    --project-root DIR    Project root directory (default: .)
    -h, --help           Show this help message

Examples:
    $0                           # Deploy with tests
    $0 --skip-tests             # Deploy without tests
    $0 --project-root /path/to/project
EOF
}

# Парсинг аргументов командной строки
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --skip-tests)
                SKIP_TESTS=true
                shift
                ;;
            --project-root)
                PROJECT_ROOT="$2"
                shift 2
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                log "ERROR" "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
}

# Инициализация путей
init_paths() {
    PROJECT_ROOT=$(realpath "$PROJECT_ROOT")
    BUILD_DIR="$PROJECT_ROOT/cpp_data_processor/build"
    TARGET_DIR="$PROJECT_ROOT"
    
    log "INFO" "Project root: $PROJECT_ROOT"
    log "INFO" "Build directory: $BUILD_DIR"
    log "INFO" "Target directory: $TARGET_DIR"
}

# Проверка зависимостей
check_dependencies() {
    log "INFO" "Checking dependencies..."
    
    local missing_deps=()
    
    # Проверка cmake
    if command -v cmake >/dev/null 2>&1; then
        local cmake_version=$(cmake --version | head -n1 | cut -d' ' -f3)
        log "INFO" "✓ cmake: $cmake_version"
    else
        log "ERROR" "✗ cmake: not found"
        missing_deps+=("cmake")
    fi
    
    # Проверка gcc
    if command -v gcc >/dev/null 2>&1; then
        local gcc_version=$(gcc --version | head -n1 | cut -d' ' -f4)
        log "INFO" "✓ gcc: $gcc_version"
    else
        log "ERROR" "✗ gcc: not found"
        missing_deps+=("gcc")
    fi
    
    # Проверка python3
    if command -v python3 >/dev/null 2>&1; then
        local python_version=$(python3 --version | cut -d' ' -f2)
        log "INFO" "✓ python3: $python_version"
    else
        log "ERROR" "✗ python3: not found"
        missing_deps+=("python3")
    fi
    
    # Проверка pip
    if command -v pip >/dev/null 2>&1 || command -v pip3 >/dev/null 2>&1; then
        local pip_version
        if command -v pip >/dev/null 2>&1; then
            pip_version=$(pip --version | cut -d' ' -f2)
        else
            pip_version=$(pip3 --version | cut -d' ' -f2)
        fi
        log "INFO" "✓ pip: $pip_version"
    else
        log "ERROR" "✗ pip: not found"
        missing_deps+=("pip")
    fi
    
    if [ ${#missing_deps[@]} -gt 0 ]; then
        log "ERROR" "Missing dependencies: ${missing_deps[*]}"
        return 1
    fi
    
    return 0
}

# Получение пути к CMake директории pybind11
get_pybind11_cmake_dir() {
    local cmake_dir
    if cmake_dir=$(python3 -c "import pybind11; print(pybind11.get_cmake_dir())" 2>/dev/null); then
        echo "$cmake_dir"
    else
        # Fallback путь
        echo "/usr/local/lib/python3.12/site-packages/pybind11/share/cmake/pybind11"
    fi
}

# Сборка C++ модуля
build_cpp_module() {
    log "INFO" "Building C++ module..."

    # Создание директории сборки
    mkdir -p "$BUILD_DIR"

    # Получение количества ядер CPU
    local num_cores=$(nproc 2>/dev/null || echo "1")
    
    # Конфигурация CMake
    log "INFO" "Configuring CMake..."
    if ! (cd "$BUILD_DIR" && cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -Dpybind11_DIR="$(get_pybind11_cmake_dir)") 2>&1; then
        log "ERROR" "CMake configuration failed"
        return 1
    fi
    
    log "INFO" "✓ CMake configuration successful"
    
    # Сборка
    log "INFO" "Building with make -j$num_cores..."
    if ! (cd "$BUILD_DIR" && make -j"$num_cores") 2>&1; then
        log "ERROR" "C++ module build failed"
        return 1
    fi
    
    log "INFO" "✓ C++ module built successfully"
    return 0
}

# Развертывание модуля
deploy_module() {
    log "INFO" "Deploying C++ module..."
    
    # Поиск собранного модуля
    local so_files=("$BUILD_DIR"/*.so)
    if [ ! -f "${so_files[0]}" ]; then
        log "ERROR" "No .so files found in build directory"
        return 1
    fi
    
    local so_file="${so_files[0]}"
    local so_filename=$(basename "$so_file")
    log "INFO" "Found module: $so_filename"
    
    # Копирование модуля в целевую директорию
    local target_file="$TARGET_DIR/$so_filename"
    if ! cp "$so_file" "$target_file"; then
        log "ERROR" "Failed to copy module to $target_file"
        return 1
    fi
    
    log "INFO" "✓ Module deployed to $target_file"
    return 0
}

# Тестирование модуля
test_module() {
    log "INFO" "Testing deployed module..."
    
    local test_script="
import sys
sys.path.insert(0, '.')
try:
    import cpp_data_processor
    print('SUCCESS: C++ module imported successfully')
    processor = cpp_data_processor.DataProcessor()
    print('SUCCESS: DataProcessor created successfully')
except Exception as e:
    print(f'ERROR: {e}')
    sys.exit(1)
"
    
    if ! (cd "$TARGET_DIR" && python3 -c "$test_script") 2>&1; then
        log "ERROR" "Module test failed"
        return 1
    fi
    
    log "INFO" "✓ Module test passed"
    return 0
}

# Запуск теста производительности
run_performance_test() {
    log "INFO" "Running performance test..."
    
    local test_script_module="test.test_build"
    
    if ! (cd "$PROJECT_ROOT" && python3 -u -m "$test_script_module") 2>&1; then
        log "ERROR" "Performance test failed"
        return 1
    fi
    
    log "INFO" "✓ Performance test passed"
    return 0
}

# Полное развертывание
deploy() {
    log "INFO" "Starting C++ Data Processor deployment"
    log "INFO" "=================================================="
    
    # Проверка зависимостей
    log "INFO" ""
    log "INFO" "--- Checking dependencies ---"
    if ! check_dependencies; then
        log "ERROR" "Deployment failed at step: Checking dependencies"
        return 1
    fi
    
    # Сборка C++ модуля
    log "INFO" ""
    log "INFO" "--- Building C++ module ---"
    if ! build_cpp_module; then
        log "ERROR" "Deployment failed at step: Building C++ module"
        return 1
    fi
    
    # Развертывание модуля
    log "INFO" ""
    log "INFO" "--- Deploying module ---"
    if ! deploy_module; then
        log "ERROR" "Deployment failed at step: Deploying module"
        return 1
    fi
    
    # Тесты (если не пропущены)
    if [ "$SKIP_TESTS" = false ]; then
        # Тестирование модуля
        log "INFO" ""
        log "INFO" "--- Testing module ---"
        if ! test_module; then
            log "ERROR" "Deployment failed at step: Testing module"
            return 1
        fi
        
        # Тест производительности
        log "INFO" ""
        log "INFO" "--- Running performance test ---"
        if ! run_performance_test; then
            log "ERROR" "Deployment failed at step: Running performance test"
            return 1
        fi
    fi
    
    log "INFO" ""
    log "INFO" "=================================================="
    log "SUCCESS" "C++ Data Processor deployed successfully!"
    return 0
}

# Главная функция
main() {
    parse_arguments "$@"
    init_paths
    
    if deploy; then
        exit 0
    else
        exit 1
    fi
}

# Запуск скрипта
main "$@"
