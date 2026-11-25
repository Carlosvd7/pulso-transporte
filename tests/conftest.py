from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixture_bytes():
    """Mensajes reales de Renfe (CC-BY 4.0) capturados el 2026-11-25 ~22:44 UTC."""

    def _load(name: str) -> bytes:
        return (FIXTURES / f"renfe_{name}.pb").read_bytes()

    return _load
