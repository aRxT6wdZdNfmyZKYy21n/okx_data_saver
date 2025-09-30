#!/usr/bin/env python3
"""
Setup script for C++ Data Processor Python bindings
"""

import os
import sys
import subprocess
import platform
from pathlib import Path

from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext
from pybind11.setup_helpers import Pybind11Extension, build_ext
from pybind11 import get_cmake_dir

# Get the long description from the README file
def read_readme():
    readme_path = Path(__file__).parent / "README.md"
    if readme_path.exists():
        return readme_path.read_text(encoding="utf-8")
    return "C++ Data Processor for OKX Data Saver"

# Define the extension module
ext_modules = [
    Pybind11Extension(
        "cpp_data_processor",
        [
            "src/python_bindings.cpp",
            "src/data_structures.cpp",
            "src/bollinger_bands.cpp",
            "src/candles_processor.cpp",
            "src/main_processor.cpp",
        ],
        include_dirs=[
            "include",
            # Add pybind11 include directory
            get_cmake_dir() / ".." / "include",
        ],
        cxx_std=17,
        define_macros=[("VERSION_INFO", '"dev"')],
    ),
]

# Custom build command
class BuildExt(build_ext):
    """Custom build command for C++ extensions"""
    
    def build_extensions(self):
        # Set compiler flags
        if platform.system() == "Windows":
            # Windows-specific flags
            for ext in self.extensions:
                ext.extra_compile_args = ["/std:c++17", "/O2"]
        else:
            # Unix-like systems
            for ext in self.extensions:
                ext.extra_compile_args = [
                    "-std=c++17",
                    "-O3",
                    "-Wall",
                    "-Wextra",
                    "-Wpedantic",
                    "-fPIC",
                ]
                ext.extra_link_args = ["-fPIC"]
        
        super().build_extensions()

# Setup configuration
setup(
    name="cpp_data_processor",
    version="1.0.0",
    author="OKX Data Saver Team",
    author_email="team@okx-data-saver.com",
    description="High-performance C++ data processor for OKX Data Saver",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/okx_data_saver",
    ext_modules=ext_modules,
    cmdclass={"build_ext": BuildExt},
    zip_safe=False,
    python_requires=">=3.7",
    install_requires=[
        "pybind11>=2.10.0",
        "numpy>=1.21.0",
        "pandas>=1.3.0",
        "polars>=0.20.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-benchmark>=4.0.0",
            "cmake>=3.15",
        ],
        "test": [
            "pytest>=7.0.0",
            "pytest-benchmark>=4.0.0",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: C++",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords="trading, finance, data-processing, c++, pybind11, okx",
    project_urls={
        "Bug Reports": "https://github.com/your-org/okx_data_saver/issues",
        "Source": "https://github.com/your-org/okx_data_saver",
        "Documentation": "https://github.com/your-org/okx_data_saver/wiki",
    },
)
