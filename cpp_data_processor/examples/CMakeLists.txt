cmake_minimum_required(VERSION 3.15...4.0)

# Example: Basic usage of the C++ data processor
add_executable(basic_example basic_example.cpp)
target_link_libraries(basic_example PRIVATE cpp_data_processor)
