cmake_minimum_required(VERSION 3.15...4.0)

# Find testing framework (Catch2 or Google Test)
find_package(Catch2 QUIET)
if(Catch2_FOUND)
    # Use Catch2 for testing
    add_executable(test_cpp_data_processor test_main.cpp)
    target_link_libraries(test_cpp_data_processor PRIVATE cpp_data_processor Catch2::Catch2)
    
    # Add test cases
    add_executable(test_bollinger_bands test_bollinger_bands.cpp)
    target_link_libraries(test_bollinger_bands PRIVATE cpp_data_processor Catch2::Catch2)
    
    add_executable(test_candles_processor test_candles_processor.cpp)
    target_link_libraries(test_candles_processor PRIVATE cpp_data_processor Catch2::Catch2)
    
    add_executable(test_data_structures test_data_structures.cpp)
    target_link_libraries(test_data_structures PRIVATE cpp_data_processor Catch2::Catch2)
    
    # Enable testing
    enable_testing()
    add_test(NAME test_cpp_data_processor COMMAND test_cpp_data_processor)
    add_test(NAME test_bollinger_bands COMMAND test_bollinger_bands)
    add_test(NAME test_candles_processor COMMAND test_candles_processor)
    add_test(NAME test_data_structures COMMAND test_data_structures)
    
else()
    # Fallback to simple test executable
    add_executable(test_cpp_data_processor test_main.cpp)
    target_link_libraries(test_cpp_data_processor PRIVATE cpp_data_processor)
    
    message(STATUS "Catch2 not found, using simple test framework")
endif()
