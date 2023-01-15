"""Pytest configuration file"""
import pandas as pd
import pytest

from life_expectancy.country_enum import Country
from . import FIXTURES_DIR, OUTPUT_DIR

@pytest.fixture(autouse=True)
def run_before_and_after_tests() -> None:
    """Fixture to execute commands before and after a test is run"""
    # Setup: fill with any logic you want

    yield # this is where the testing happens

    # Teardown : fill with any logic you want
    file_path = OUTPUT_DIR / "pt_life_expectancy.csv"
    file_path.unlink(missing_ok=True)


@pytest.fixture(scope="session")
def pt_life_expectancy_expected() -> pd.DataFrame:
    """Fixture to load the expected output of the cleaning script."""

    expected_df = pd.read_csv(
        FIXTURES_DIR / "pt_life_expectancy_expected.csv")

    expected_df = (
        expected_df.
        fillna(0).
        astype({"year":"int64", "value": "float64"})
    )

    return expected_df

@pytest.fixture(scope="session")
def pt_life_expectancy_input_csv() -> pd.DataFrame:
    """Fixture to load and clean the original dataframe
    and only keep the first 30 rows."""

    created_df = pd.read_csv(
        OUTPUT_DIR.joinpath("eu_life_expectancy_raw.tsv"),
        sep="\t")
    return created_df

@pytest.fixture(scope="session")
def pt_life_expectancy_input_json() -> pd.DataFrame:
    """Fixture to load and clean the original dataframe
    and only keep the first 30 rows."""

    created_df = pd.read_json(
        OUTPUT_DIR.joinpath("eurostat_life_expect.json"),
        typ="frame")

    return created_df

@pytest.fixture(scope="session")
def monkeypatch() -> pytest.MonkeyPatch:
    """Instantiating MonkeyPatch object."""

    patch = pytest.MonkeyPatch()

    return patch

@pytest.fixture(scope="session")
def eu_life_expectancy_raw() -> pd.DataFrame:
    """Fixture to load the expected output of the cleaning script"""
    baseline_df = pd.read_csv(
        FIXTURES_DIR / "eu_life_expectancy_expected.csv")

    return  baseline_df

@pytest.fixture(scope="session")
def countries()-> Country:
    """Returns Country class."""
    return Country
