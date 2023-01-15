"""Test enum Country class."""
import pandas as pd
from life_expectancy.country_enum import Country

def test_countries_enum(
    pt_life_expectancy_input_csv : pd.DataFrame,
    countries: Country
    ) -> None :
    """Test if countries in Country class are as expected."""
    pt_life_expectancy_input_csv.columns =  [
    column_title.replace("\\","") for column_title
    in pt_life_expectancy_input_csv.columns
    ]

    #DataFrame had multiple columns for years, thus melting columns into rows:
    year_cols = pt_life_expectancy_input_csv.columns[1:]
    pt_life_expectancy_input_csv = pd.melt(pt_life_expectancy_input_csv,
                            id_vars= pt_life_expectancy_input_csv.columns[0],
                            var_name= "year",
                            value_vars= year_cols,
                            value_name= "value")

    pt_life_expectancy_input_csv[["unit","sex","age","region"]] = (
        pt_life_expectancy_input_csv["unit,sex,age,geotime"]
        .str.split(',', expand=True)
        )
    pt_life_expectancy_input_csv = (
        pt_life_expectancy_input_csv[
            ~pt_life_expectancy_input_csv["region"]
            .str.contains(pat=r"\d+", regex = True)]
    )
    list_of_countries = (
        list(pt_life_expectancy_input_csv[
            pt_life_expectancy_input_csv["region"]!="EFTA"]
            ["region"].unique())
    )
    class_countries = [country.value for country in countries]

    assert list_of_countries.sort() == class_countries.sort()
