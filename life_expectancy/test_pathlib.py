"""Test pathlib."""

import pathlib

PATH = pathlib.Path.cwd() / 'life_expectancy'

PATH.joinpath('data', 'pt_life_expectancy.csv')
print(pathlib.Path(__file__))