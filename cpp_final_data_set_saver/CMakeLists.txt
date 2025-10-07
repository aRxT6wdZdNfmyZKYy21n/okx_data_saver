cmake_minimum_required(VERSION 3.16)
project(okx_final_data_set_saver VERSION 1.0.0 LANGUAGES CXX)

# Установка стандарта C++
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Настройки компилятора
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O2")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG")

# Поиск зависимостей
find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)

# PostgreSQL (libpqxx)
pkg_check_modules(PQXX REQUIRED libpqxx)

# spdlog для логирования
find_package(spdlog REQUIRED)

# nlohmann/json для работы с JSON
find_package(nlohmann_json REQUIRED)

# Boost для Multiprecision
find_package(Boost REQUIRED COMPONENTS system)

# Создание исполняемого файла
add_executable(okx_final_data_set_saver
    src/main.cpp
    src/database/database_connection.cpp
    src/database/database_transaction.cpp
    src/database/database_query.cpp
    src/processors/data_set_calculator.cpp
    src/processors/main_loop.cpp
    src/utils/config.cpp
    src/utils/logger.cpp
    src/utils/decimal.cpp
)

# Включение заголовочных файлов
target_include_directories(okx_final_data_set_saver PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/external
)

# Подключение библиотек
target_link_libraries(okx_final_data_set_saver
    ${PQXX_LIBRARIES}
    Threads::Threads
    spdlog::spdlog
    nlohmann_json::nlohmann_json
    Boost::system
)

# Компиляционные флаги
target_compile_options(okx_final_data_set_saver PRIVATE ${PQXX_CFLAGS_OTHER})

# Тесты
enable_testing()
add_subdirectory(test)

# Примеры
add_subdirectory(examples)

# Установка
install(TARGETS okx_final_data_set_saver
    RUNTIME DESTINATION bin
)
