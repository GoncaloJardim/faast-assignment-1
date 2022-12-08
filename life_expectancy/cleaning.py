"""Cleaning Life Expectancy Data for Assignment 1- Data Cleaning Challenge."""
# pylint: disable=invalid-name
import argparse
import pandas as pd
import numpy as np
from life_expectancy.config_load import ConfigLoad, DATA_PATH
from life_expectancy.country_enum import Country


def load_data(file_type : str = "csv") -> pd.DataFrame:
    """
    Loads the data into a DataFrame. Source file is dependent of file_type.
    Args:
        -file_type (str): specifies the file format to load. 'csv' as default,
            may also accept 'json'.
    Returns:
        -pd.DataFrame : Loaded table."""

    configuration = ConfigLoad(file_type)
    return configuration.choose_and_load()



def clean_csv_data(
    life_expectancy: pd.DataFrame,
    region: str = Country.PT) -> pd.DataFrame:
    """ Clean_data function does the following for DataFrame of csv origin:
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
        life_expectancy["unit,sex,age,geotime"]
        .str.split(',', expand=True)
        )
    #As a lot of cells are just filled with ': ', we replace with NaN
    #and then drop any row that has NaN cells
    life_expectancy = (
        life_expectancy
        .drop(columns="unit,sex,age,geotime")
        .replace(": ",np.NaN)
        .dropna(how="any")
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

def clean_json_data(
    life_expectancy: pd.DataFrame,
    region: str = Country.PT) -> pd.DataFrame:
    """Cleans DataFrame from json origin.
    -Arg:
        - life_expectancy (pd.DataFrame): DataFrame loaded from load_json function.
        - region (Country): Country initials from which to filter the DataFrame. Default is PT.
    -Returns:
        - life_expectancy (pd.DataFrame): transformed DataFrame based on assigment criteria.
    """

    life_expectancy = (
        life_expectancy[life_expectancy['country'] == region]
        .rename(columns={
            'country': 'region',
            'life_expectancy': 'value'})
        .drop(columns=["flag", "flag_detail"])
        .astype({
            "year":"int64",
            "value": "float64"})
    )

    return life_expectancy

def clean_data(
    life_expectancy: pd.DataFrame,
    file_type: str,
    region: str = Country.PT):
    """Selects and runs apropriate cleaning function based on origin file type.
    -Args:
        -life_expectancy (pd.DataFrame): DataFrame loaded.
        -file_type (str) : Type of file, csv or json format.
        -region (str) : Country initials on which to filter. Default is 'PT'.
    -Returns:
        -life_expectancy (pd.DataFrame): Transformed and cleaned DataFrame.
    """

    if file_type == "csv":
        df_ = clean_csv_data(life_expectancy, region)
        return df_

    if file_type == "json":
        return clean_json_data(life_expectancy, region)

    return ValueError("""
            Invalid format was provided. Pass arg. 'json' or 'csv'""")

def save_data(life_expectancy: pd.DataFrame) -> None:
    """Saves DataFrame into desired directory."""

    return  life_expectancy.to_csv(
        DATA_PATH.joinpath("pt_life_expectancy.csv"),
        index= False)


def main(
    file_type: str = "csv",
    region: Country = Country.PT,
    ) -> str:
    """Pipeline with functions for:
    - Loading data;
    - Cleaning data;
    - Saving data;"""

    valid_countries = [member.value for member in Country]

    if region in valid_countries:

        eurostat_df = load_data(file_type)
        eurostat_df = clean_data(eurostat_df, file_type, region)
        eurostat_df = save_data(eurostat_df)

    else:
        print(f"""
        Incorrect country.
        Please pass one of these countries : {valid_countries}""")




if __name__ == "__main__": # pragma: no cover

    parser = argparse.ArgumentParser(description='Clean data based on specific Region.')
    parser.add_argument(
        '-r',
        '--region',
        type=str,
        help="Indicate preferred country with 2 capital letters (default= 'PT').",
        default=Country.PT,
    )
    parser.add_argument(
        '-f',
        '--format_type',
        type=str,
        help="Select source file format. Default is csv, may also choose json",
        default="csv",
    )
    args = parser.parse_args()

    region_letters = args.region
    file_format = args.format_type

    #Run pipeline:

    main(file_format, region_letters)
