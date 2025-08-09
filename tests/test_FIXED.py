# Testing suite for column type: EXPRESSION

from datafaker.SchemaParser import *
import pytest

# Unit tests for SchemaColumnFixed

def test_SchemaColumnFixed_valid_str():
    column = SchemaColumnFixed(name = "column_test", value = "test")
    assert isinstance(column, SchemaColumnFixed)
    assert vars(column) == {"_name" : "column_test", "_value" : "test"}

def test_SchemaColumnFixed_valid_int():
    column = SchemaColumnFixed(name = "column_test", value = 10)
    assert isinstance(column, SchemaColumnFixed)
    assert vars(column) == {"_name" : "column_test", "_value" : 10}

def test_SchemaColumnFixed_valid_float():
    column = SchemaColumnFixed(name = "column_test", value = 5.1)
    assert isinstance(column, SchemaColumnFixed)
    assert vars(column) == {"_name" : "column_test", "_value" : 5.1}

def test_SchemaColumnFixed_valid_datetime():
    column = SchemaColumnFixed(name = "column_test", value = datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0))
    assert isinstance(column, SchemaColumnFixed)
    assert vars(column) == {"_name" : "column_test", "_value" : datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0)}

def test_SchemaColumnFixed_valid_date():
    column = SchemaColumnFixed(name = "column_test", value = date(year=2025, month=1, day=1))
    assert isinstance(column, SchemaColumnFixed)
    assert vars(column) == {"_name" : "column_test", "_value" : date(year=2025, month=1, day=1)}

# NOTE This is different than passing `value=""`
# TODO I think we should treat both cases as identical and allow for empty values (null in spark)
def test_SchemaColumnFixed_missing_value():
    with pytest.raises(TypeError):
        SchemaColumnFixed(name = "column_test")


# Unit tests for `parse_column_type()` for Fixed Columns

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

# Integration tests for `parse_column_type()` for Fixed columns

column_fixed_valid_int = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Fixed",
    "value": 10
}

column_fixed_valid_float = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Fixed",
    "value": 5.1
}

column_fixed_valid_string = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Fixed",
    "value": "string"
}

column_fixed_valid_datetime = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Fixed",
    "value": datetime.fromisoformat("2025-01-01T00:00:00")
}

column_fixed_valid_date = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Fixed",
    "value": date.fromisoformat("2025-01-01")
}

@pytest.mark.parametrize("input, result", [
    (column_fixed_valid_int, {"_name" : "column_test", "_value" : 10}),
    (column_fixed_valid_float, {"_name" : "column_test", "_value" : 5.1}),
    (column_fixed_valid_string, {"_name" : "column_test", "_value" : "string"}),
    (column_fixed_valid_datetime, {"_name" : "column_test", "_value" : datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0)}),
    (column_fixed_valid_date, {"_name" : "column_test", "_value" : date(year=2025, month=1, day=1)})
])
def test_parse_column_fixed_val_par(input, result):
    column = parse_column_type(input)
    assert isinstance(column, SchemaColumnFixed)
    assert vars(column) == result