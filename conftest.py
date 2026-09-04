"""Делает модули из корня репозитория импортируемыми в тестах.

Проект не устанавливается как пакет (`pip install -e .`), поэтому
`import generate_data` из tests/ работает только через этот путь.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
