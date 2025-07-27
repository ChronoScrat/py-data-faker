# Testing Suite for SchemaParser

from datafaker.SchemaParser import *
import pytest


# `parse_column_type()`
# NOTE Individual tests for each column type are present in the `test_{COLUMN_TYPE}.py` files.


column_random_valid = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Random",
    "min": 0,
    "max" : 1
}

column_random_missing_type = {
    "name": "column_test",
    "data_type": "Int",
    "min": 0,
    "max" : 1
}

column_random_invalid_type = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "MyCustomType",
    "min": 0,
    "max" : 1
}

def test_parse_column_valid():
    random = parse_column_type(column_random_valid)
    assert isinstance(random, SchemaColumnRandom)
    assert vars(random) == {"_name" : "column_test", "_min": 0, "_max" : 1}

def test_parse_column_missing_type():
    with pytest.raises(ValueError):
        parse_column_type(column_random_missing_type)

# def test_parse_column_invalid_type():
#     with pytest.raises(ValueError):
#         parse_column_type(column_random_invalid_type)



# `parse_schema_from_file()`

import yaml
from pathlib import Path

schema_test_file = Path(__file__).parent/"example_schema_test.yaml"

def test_parse_schema_from_file():
    schema_test = parse_schema_from_file(schema_test_file)
    assert isinstance(schema_test, Schema)

