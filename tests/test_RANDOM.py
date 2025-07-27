# Testing suite for `parse_colum_type()` from SchemaParser.py
# Column Type: RANDOM

from datafaker.SchemaParser import *
import pytest


column_random_valid = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Random",
    "min": 0,
    "max" : 1
}

column_random_missing_min = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Random",
    "max" : 1
}

column_random_missing_max = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Random",
    "min": 0
}

column_random_missing_both = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Random"
}


def test_parse_column_random_valid():
    random = parse_column_type(column_random_valid)
    assert isinstance(random, SchemaColumnRandom)
    assert vars(random) == {"_name" : "column_test", "_min": 0, "_max" : 1}

def test_parse_column_random_missing_min():
    with pytest.raises(ValueError):
        parse_column_type(column_random_missing_min)

def test_parse_column_random_missing_max():
    with pytest.raises(ValueError):
        parse_column_type(column_random_missing_max)

def test_parse_column_random_missing_both():
    with pytest.raises(ValueError):
        parse_column_type(column_random_missing_both)

