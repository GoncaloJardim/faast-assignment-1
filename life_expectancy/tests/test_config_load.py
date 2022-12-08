"""Tests for the config_load class module."""
import pytest
from life_expectancy.config_load import ConfigLoad

def test_load_data_csv(
    monkeypatch : pytest.MonkeyPatch) -> None :
    """Patch read_csv method, testing load_data_csv function."""

    def _mockreturn_load_csv(*args, **kwargs) -> str :
        """Result to receive when mock."""
        # pylint: disable=unused-argument

        return "DataFrame loaded from csv file."

    monkeypatch.setattr(
        "life_expectancy.config_load.pd.read_csv",
        _mockreturn_load_csv)

    assert ConfigLoad('csv').load_csv() == "DataFrame loaded from csv file."

def test_load_data_json(
    monkeypatch : pytest.MonkeyPatch) -> None :
    """Patch read_json method, testing load_data_json function."""

    def _mockreturn_load_json(*args, **kwargs) -> str :
        """Result to receive when mock."""
        # pylint: disable=unused-argument

        return "DataFrame loaded."

    monkeypatch.setattr(
        "life_expectancy.config_load.pd.read_json",
        _mockreturn_load_json)

    assert ConfigLoad('json').load_json() == "DataFrame loaded."
