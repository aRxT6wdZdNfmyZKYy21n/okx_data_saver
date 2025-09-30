cmake_minimum_required(VERSION 3.15...4.0)

project(cpp_data_processor 
    VERSION 1.0.0
    LANGUAGES CXX
    DESCRIPTION "C++ Data Processor for OKX Data Saver with Python bindings"
)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set build type if not specified
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Find pybind11
set(PYBIND11_FINDPYTHON ON)
find_package(pybind11 CONFIG REQUIRED)

# Find Python
find_package(Python COMPONENTS Interpreter Development REQUIRED)

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# Source files
set(SOURCES
    src/data_structures.cpp
    src/bollinger_bands.cpp
    src/candles_processor.cpp
    src/rsi_calculator.cpp
    src/smoothing_processor.cpp
    src/extreme_lines_processor.cpp
    src/order_book_processor.cpp
    src/velocity_calculator.cpp
    src/main_processor.cpp
    src/python_bindings.cpp
)

# Header files
set(HEADERS
    include/data_structures.h
    include/bollinger_bands.h
    include/candles_processor.h
    include/rsi_calculator.h
    include/smoothing_processor.h
    include/extreme_lines_processor.h
    include/order_book_processor.h
    include/velocity_calculator.h
    include/main_processor.h
)

# Create the Python extension module
pybind11_add_module(cpp_data_processor SHARED ${SOURCES} ${HEADERS})

# Set target properties
set_target_properties(cpp_data_processor PROPERTIES
    OUTPUT_NAME "cpp_data_processor"
    CXX_VISIBILITY_PRESET "hidden"
    VISIBILITY_INLINES_HIDDEN ON
)

# Link libraries
target_link_libraries(cpp_data_processor PRIVATE pybind11::module)

# Compiler-specific options
if(MSVC)
    target_compile_options(cpp_data_processor PRIVATE /W4)
else()
    target_compile_options(cpp_data_processor PRIVATE -Wall -Wextra -Wpedantic)
endif()

# Install targets
install(TARGETS cpp_data_processor DESTINATION .)

# Create test target if testing is enabled
if(BUILD_TESTING)
    enable_testing()
    add_subdirectory(tests)
endif()

# Create example target (optional)
option(BUILD_EXAMPLES "Build examples" OFF)
if(BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()
