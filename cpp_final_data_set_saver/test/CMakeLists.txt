# Поиск Google Test
find_package(GTest REQUIRED)

# Unit тесты
add_executable(unit_tests
    unit/test_config.cpp
    unit/test_database_connection.cpp
    unit/test_data_set_calculator.cpp
    unit/test_performance.cpp
    unit/test_calculations.cpp
    unit/test_decimal.cpp
    unit/test_database_transaction.cpp
    unit/test_database_query.cpp
)

target_include_directories(unit_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

# Добавляем исходные файлы для тестов
target_sources(unit_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/processors/data_set_calculator.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/utils/logger.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/utils/config.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/utils/decimal.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/database/database_connection.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/database/database_transaction.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/database/database_query.cpp
)

target_link_libraries(unit_tests
    GTest::gtest
    GTest::gtest_main
    ${PQXX_LIBRARIES}
    Threads::Threads
    spdlog::spdlog
    nlohmann_json::nlohmann_json
)

# Интеграционные тесты
add_executable(integration_tests
    integration/test_database_integration.cpp
)

target_include_directories(integration_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(integration_tests
    GTest::gtest
    GTest::gtest_main
    ${PQXX_LIBRARIES}
    Threads::Threads
    spdlog::spdlog
    nlohmann_json::nlohmann_json
)

# Запуск тестов
add_test(NAME unit_tests COMMAND unit_tests)
add_test(NAME integration_tests COMMAND integration_tests)
