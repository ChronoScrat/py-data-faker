# Testing suite for `parse_colum_type()` from SchemaParser.py
# Column Type: SELECTION

from datafaker.SchemaParser import *
import pytest


column_selection_valid = {
    "name": "column_test",
    "data_type": "String",
    "column_type" : "Selection",
    "values": [
        "value1",
        "value2",
        "value3"
    ]
}

column_selection_missing_values = {
    "name": "column_test",
    "data_type": "String",
    "column_type" : "Selection"
}

def test_parse_column_selection_valid():
    selection = parse_column_type(column_selection_valid)
    assert isinstance(selection, SchemaColumnSelection)
    assert vars(selection) == {"_name" : "column_test", "_values": ["value1","value2","value3"]}

def test_parse_column_selection_missing_values():
    with pytest.raises(ValueError):
        parse_column_type(column_selection_missing_values)

