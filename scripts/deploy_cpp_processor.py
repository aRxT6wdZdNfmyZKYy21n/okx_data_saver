#!/usr/bin/env python3
"""
Deployment Script - скрипт для развертывания и обновления C++ data processor.
"""

import os
import sys
import shutil
import subprocess
import logging
from pathlib import Path
from typing import Optional, List

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s][%(asctime)s][%(name)s]: %(message)s'
)
logger = logging.getLogger(__name__)


class CppProcessorDeployer:
    """Класс для развертывания C++ data processor."""
    
    def __init__(self, project_root: str):
        """
        Инициализация деплоера.
        
        Args:
            project_root: Корневая директория проекта
        """
        self.project_root = Path(project_root)
        self.cpp_dir = self.project_root / "cpp_data_processor"
        self.build_dir = self.cpp_dir / "build"
        self.target_dir = self.project_root
        
    def check_dependencies(self) -> bool:
        """
        Проверка зависимостей для сборки C++ модуля.
        
        Returns:
            bool: True если все зависимости установлены
        """
        logger.info("Checking dependencies...")
        
        dependencies = [
            ("cmake", "cmake --version"),
            ("gcc", "gcc --version"),
            ("python3", "python3 --version"),
            ("pip", "pip --version")
        ]
        
        missing_deps = []
        
        for dep_name, check_cmd in dependencies:
            try:
                result = subprocess.run(check_cmd.split(), 
                                      capture_output=True, text=True, check=True)
                logger.info(f"✓ {dep_name}: {result.stdout.split()[0]}")
            except (subprocess.CalledProcessError, FileNotFoundError):
                logger.error(f"✗ {dep_name}: not found")
                missing_deps.append(dep_name)
        
        if missing_deps:
            logger.error(f"Missing dependencies: {', '.join(missing_deps)}")
            return False
        
        return True
    
    def install_python_dependencies(self) -> bool:
        """
        Установка Python зависимостей.
        
        Returns:
            bool: True если установка прошла успешно
        """
        logger.info("Installing Python dependencies...")
        
        try:
            # Установка pybind11
            result = subprocess.run([
                "python3", "-m", "pip", "install", "pybind11", "--break-system-packages"
            ], check=True, capture_output=True, text=True)
            
            logger.info("✓ pybind11 installed successfully")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to install Python dependencies: {e}")
            return False
    
    def build_cpp_module(self) -> bool:
        """
        Сборка C++ модуля.
        
        Returns:
            bool: True если сборка прошла успешно
        """
        logger.info("Building C++ module...")
        
        try:
            # Создание директории сборки
            self.build_dir.mkdir(exist_ok=True)
            
            # Конфигурация CMake
            cmake_cmd = [
                "cmake", "..", 
                "-DCMAKE_BUILD_TYPE=Release",
                f"-Dpybind11_DIR={self._get_pybind11_cmake_dir()}"
            ]
            
            result = subprocess.run(cmake_cmd, cwd=self.build_dir, 
                                  check=True, capture_output=True, text=True)
            logger.info("✓ CMake configuration successful")
            
            # Сборка
            import os
            num_cores = os.cpu_count() or 1
            result = subprocess.run(["make", f"-j{num_cores}"], cwd=self.build_dir,
                                  check=True, capture_output=True, text=True)
            logger.info("✓ C++ module built successfully")
            
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to build C++ module: {e}")
            if e.stdout:
                logger.error(f"STDOUT: {e.stdout}")
            if e.stderr:
                logger.error(f"STDERR: {e.stderr}")
            return False
    
    def _get_pybind11_cmake_dir(self) -> str:
        """
        Получение пути к CMake директории pybind11.
        
        Returns:
            str: Путь к CMake директории pybind11
        """
        try:
            result = subprocess.run([
                "python3", "-c", "import pybind11; print(pybind11.get_cmake_dir())"
            ], capture_output=True, text=True, check=True)
            return result.stdout.strip()
        except subprocess.CalledProcessError:
            # Fallback путь
            return "/usr/local/lib/python3.12/site-packages/pybind11/share/cmake/pybind11"
    
    def deploy_module(self) -> bool:
        """
        Развертывание собранного модуля.
        
        Returns:
            bool: True если развертывание прошло успешно
        """
        logger.info("Deploying C++ module...")
        
        try:
            # Поиск собранного модуля
            so_files = list(self.build_dir.glob("*.so"))
            if not so_files:
                logger.error("No .so files found in build directory")
                return False
            
            so_file = so_files[0]
            logger.info(f"Found module: {so_file.name}")
            
            # Копирование модуля в целевую директорию
            target_file = self.target_dir / so_file.name
            shutil.copy2(so_file, target_file)
            
            logger.info(f"✓ Module deployed to {target_file}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to deploy module: {e}")
            return False
    
    def test_module(self) -> bool:
        """
        Тестирование развернутого модуля.
        
        Returns:
            bool: True если тест прошел успешно
        """
        logger.info("Testing deployed module...")
        
        try:
            # Простой тест импорта
            test_script = """
import sys
sys.path.insert(0, '.')
try:
    import cpp_data_processor
    print("SUCCESS: C++ module imported successfully")
    processor = cpp_data_processor.DataProcessor()
    print("SUCCESS: DataProcessor created successfully")
except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)
"""
            
            result = subprocess.run([
                "python3", "-c", test_script
            ], cwd=self.target_dir, check=True, capture_output=True, text=True)
            
            logger.info("✓ Module test passed")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Module test failed: {e}")
            if e.stdout:
                logger.error(f"STDOUT: {e.stdout}")
            if e.stderr:
                logger.error(f"STDERR: {e.stderr}")
            return False
    
    def run_performance_test(self) -> bool:
        """
        Запуск теста производительности.
        
        Returns:
            bool: True если тест прошел успешно
        """
        logger.info("Running performance test...")
        
        try:
            test_script = self.project_root / "cpp_data_processor" / "test_build.py"
            if not test_script.exists():
                logger.warning("Performance test script not found, skipping")
                return True
            
            result = subprocess.run([
                "python3", str(test_script)
            ], cwd=self.project_root, check=True, capture_output=True, text=True)
            
            logger.info("✓ Performance test passed")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Performance test failed: {e}")
            return False
    
    def create_backup(self) -> bool:
        """
        Создание резервной копии текущего модуля.
        
        Returns:
            bool: True если резервная копия создана успешно
        """
        logger.info("Creating backup...")
        
        try:
            backup_dir = self.target_dir / "backup"
            backup_dir.mkdir(exist_ok=True)
            
            # Поиск существующих .so файлов
            so_files = list(self.target_dir.glob("*.so"))
            for so_file in so_files:
                backup_file = backup_dir / so_file.name
                shutil.copy2(so_file, backup_file)
                logger.info(f"✓ Backed up {so_file.name}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            return False
    
    def deploy(self, skip_tests: bool = False) -> bool:
        """
        Полное развертывание C++ процессора.
        
        Args:
            skip_tests: Пропустить тесты
            
        Returns:
            bool: True если развертывание прошло успешно
        """
        logger.info("Starting C++ Data Processor deployment")
        logger.info("=" * 50)
        
        steps = [
            ("Checking dependencies", self.check_dependencies),
            ("Installing Python dependencies", self.install_python_dependencies),
            ("Creating backup", self.create_backup),
            ("Building C++ module", self.build_cpp_module),
            ("Deploying module", self.deploy_module),
        ]
        
        if not skip_tests:
            steps.extend([
                ("Testing module", self.test_module),
                ("Running performance test", self.run_performance_test),
            ])
        
        for step_name, step_func in steps:
            logger.info(f"\n--- {step_name} ---")
            if not step_func():
                logger.error(f"Deployment failed at step: {step_name}")
                return False
        
        logger.info("\n" + "=" * 50)
        logger.info("✓ C++ Data Processor deployed successfully!")
        return True
    
    def rollback(self) -> bool:
        """
        Откат к предыдущей версии.
        
        Returns:
            bool: True если откат прошел успешно
        """
        logger.info("Rolling back to previous version...")
        
        try:
            backup_dir = self.target_dir / "backup"
            if not backup_dir.exists():
                logger.error("No backup found for rollback")
                return False
            
            # Восстановление из резервной копии
            backup_files = list(backup_dir.glob("*.so"))
            for backup_file in backup_files:
                target_file = self.target_dir / backup_file.name
                shutil.copy2(backup_file, target_file)
                logger.info(f"✓ Restored {backup_file.name}")
            
            logger.info("✓ Rollback completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Rollback failed: {e}")
            return False


def main():
    """Главная функция скрипта развертывания."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Deploy C++ Data Processor")
    parser.add_argument("--skip-tests", action="store_true", 
                       help="Skip tests during deployment")
    parser.add_argument("--rollback", action="store_true",
                       help="Rollback to previous version")
    parser.add_argument("--project-root", default=".",
                       help="Project root directory")
    
    args = parser.parse_args()
    
    deployer = CppProcessorDeployer(args.project_root)
    
    if args.rollback:
        success = deployer.rollback()
    else:
        success = deployer.deploy(skip_tests=args.skip_tests)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
