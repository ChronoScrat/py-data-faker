# Testing suite for `parse_colum_type()` from SchemaParser.py
# Column Type: FIXED

from datafaker.SchemaParser import *
import pytest


column_fixed_valid = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Fixed",
    "value": 10
}

column_fixed_missing_value = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Fixed"
}

def test_parse_column_fixed_valid():
    test_column = parse_column_type(column_fixed_valid)
    assert isinstance(test_column, SchemaColumnFixed)
    assert vars(test_column) == {"_name" : "column_test", "_value" : 10}

def test_parse_column_fixed_missing_value():
    with pytest.raises(ValueError):
        parse_column_type(column_fixed_missing_value)

