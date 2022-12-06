"""Adapters to load json and csv files."""
import pathlib
from pathlib import Path
import pandas as pd


DATA_PATH =  Path(__file__).parent / 'data'



class ConfigLoad:
    """Configures base data input of eurostat life expectancy and loads it.
    Attributes:
        file_type (str): Format type, json or csv.
            will use that to select method of loading data.
        path (pathlib.Path): Path where data resides.
            By default it's selected data folder.
    """

    def __init__(
        self,
        file_type: str,
        path: pathlib.Path = DATA_PATH):

        self.file_type : str = file_type
        self.path : pathlib.Path = path

    def load_json(self)-> pd.DataFrame:
        """loads json file in data folder"""
        return pd.read_json(
            self.path.joinpath("eurostat_life_expect.json"), typ='frame')

    def load_csv(self)-> pd.DataFrame:
        """loads csv file in data folder"""

        return  pd.read_csv(
            self.path.joinpath("eu_life_expectancy_raw.tsv"), sep="\t")

    def choose_and_load(self):
        """Based on date format, loads data for specific file."""
        if self.file_type == "csv":
            return self.load_csv()
        elif self.file_type == "json":
            return self.load_json()
        else:
            raise ValueError("""
            Invalid format was provided. Pass arg. 'json' or 'csv'""")
