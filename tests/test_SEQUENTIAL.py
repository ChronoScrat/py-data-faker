# Testing suite for `parse_colum_type()` from SchemaParser.py
# Column Type: SEQUENTIAL

from datafaker.SchemaParser import *
import pytest


column_sequential_valid = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Sequential",
    "start": 0,
    "step" : 1
}

column_sequential_missing_start = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Sequential",
    "step" : 1
}

column_sequential_missing_step = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Sequential",
    "start": 0
}

column_sequential_missing_both = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Sequential"
}

def test_parse_column_sequential_valid():
    sequential = parse_column_type(column_sequential_valid)
    assert isinstance(sequential, SchemaColumnSequential)
    assert vars(sequential) == {"_name" : "column_test", "_start": 0, "_step" : 1}

def test_parse_column_sequential_missing_start():
    with pytest.raises(ValueError):
        parse_column_type(column_sequential_missing_start)

def test_parse_column_sequential_missing_step():
    with pytest.raises(ValueError):
        parse_column_type(column_sequential_missing_step)

def test_parse_column_sequential_missing_both():
    with pytest.raises(ValueError):
        parse_column_type(column_sequential_missing_both)