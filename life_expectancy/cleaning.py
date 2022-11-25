"""Cleaning Life Expectancy Data for Assignment 1- Data Cleaning Challenge."""

import pathlib
import argparse
import pandas as pd
import numpy as np

DATA_PATH =  pathlib.Path(__file__).parent / 'data'

def load_data() -> pd.DataFrame:
    """Loads the data into a pd.DataFrame."""

    return pd.read_csv(
        DATA_PATH.joinpath("eu_life_expectancy_raw.tsv"),
        sep="\t")

def clean_data(
    life_expectancy: pd.DataFrame,
    region: str = "PT") -> pd.DataFrame:
    """ Clean_data function does the following:
        -Melts Date columns into a single column;
        -Split first column into 4 different;
        -Remove NaN's;
        -Transform columns value and year and its datatypes
        -Filter region column, only for Portugal (PT).
    """

    life_expectancy.columns =  [
        column_title.replace("\\","") for column_title
        in life_expectancy.columns
        ]

    #DataFrame had multiple columns for years, thus melting columns into rows:
    year_cols = life_expectancy.columns[1:]
    life_expectancy = pd.melt(life_expectancy,
                            id_vars= life_expectancy.columns[0],
                            var_name= "year",
                            value_vars= year_cols,
                            value_name= "value")

    life_expectancy[["unit","sex","age","region"]] = (
        life_expectancy["unit,sex,age,geotime"].
        str.split(',', expand=True)
        )
    #As a lot of cells are just filled with ': ', we replace with NaN
    #and then drop any row that has NaN cells
    life_expectancy = (
        life_expectancy.
        drop(columns="unit,sex,age,geotime").
        replace(": ",np.NaN).
        dropna(how="any")
        )

    life_expectancy["value"] = (
        life_expectancy["value"].
        str.split().str[0]
    )

    #Filter Dataframe for the region the user wants. By default its PT:
    life_expectancy = life_expectancy[life_expectancy["region"]== region]

    life_expectancy = life_expectancy.astype(
        {"year":"int64", "value": "float64"})

    life_expectancy = life_expectancy[
        ["unit","sex","age","region","year","value"]
        ]

    life_expectancy.dropna(how="any", inplace=True)

    return life_expectancy

def save_data(life_expectancy: pd.DataFrame) -> None:
    """Saves DataFrame into desired directory."""

    return  life_expectancy.to_csv(
        DATA_PATH.joinpath("pt_life_expectancy.csv"),
        index= False)


def main(region: str ="PT") -> None:
    """Pipeline with functions for:
    - Loading data;
    - Cleaning data;
    - Saving data;"""

    (
        load_data().
        pipe(clean_data, region).
        pipe(save_data)
    )

if __name__ == "__main__": # pragma: no cover

    parser = argparse.ArgumentParser(description='Clean data based on specific Region.')
    parser.add_argument(
        '-r',
        '--region',
        type=str,
        help="Indicate preferred country with 2 capital letters (default= 'PT').",
        default="PT",
    )
    args = parser.parse_args()

    region_letters = args.region

    #Run pipeline:

    main(region_letters)
