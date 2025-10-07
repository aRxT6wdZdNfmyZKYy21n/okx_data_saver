# Примеры использования

# Базовый пример
add_executable(basic_usage
    basic_usage.cpp
)

target_include_directories(basic_usage PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_sources(basic_usage PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/processors/data_set_calculator.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/utils/logger.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/utils/config.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/utils/decimal.cpp
)

target_link_libraries(basic_usage
    ${PQXX_LIBRARIES}
    Threads::Threads
    spdlog::spdlog
    nlohmann_json::nlohmann_json
)
