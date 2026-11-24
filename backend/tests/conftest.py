"""Pytest configuration for backend tests."""
import sys
from pathlib import Path

import pytest

# Ensure backend is on sys.path
backend_dir = Path(__file__).resolve().parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))


@pytest.fixture(autouse=True)
def reset_store():
    """每个测试前重置 store 为初始种子数据，保证测试隔离。"""
    from app.store import store
    store.reset()
    yield
