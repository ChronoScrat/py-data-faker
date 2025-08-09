# Testing suite for column type: SELECTION

from datafaker.SchemaParser import *
import pytest


# Unit tests for SchemaColumnSelection

@pytest.mark.parametrize("input",[
    (["value1","value2", "value3"]),
    ([1,2,3,4,5,6,7,8]),
    ([True,False]),
    ([datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0),
      datetime(year=2025, month=6, day=15, hour=12, minute=30, second=30),
      datetime(year=2025, month=12, day=31, hour=23, minute=59, second=59)])
])
def test_SchemaColumnSelection_valid(input):
    column = SchemaColumnSelection(name = "column_test", values = input)
    assert isinstance(column, SchemaColumnSelection)
    assert vars(column) == {"_name" : "column_test", "_values" : input}

def test_SchemaColumnSelection_missing_values():
    with pytest.raises(TypeError):
        SchemaColumnSelection(name = "column_test")

def test_SchemaColumnSelection_empty_values():
    with pytest.raises(TypeError):
        SchemaColumnSelection(name = "column_test", values = "")


@pytest.mark.parametrize("input",[
    (["value1",8, "value3"]),
    ([1,2,3,"4",5,6,True,8]),
    ([True,1,2,0,"string"]),
    ([datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0),"10",False])
])
def test_SchemaColumnSelection_multiple_type_list(input):
    with pytest.raises(TypeError):
        SchemaColumnSelection(name = "column_test", values = input)


# Unit tests for `parse_column_type()` for Selection Columns

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

column_selection_not_list = {
    "name": "column_test",
    "data_type": "String",
    "column_type" : "Selection",
    "values": 100
}

column_selection_empty_values = {
    "name": "column_test",
    "data_type": "String",
    "column_type" : "Selection",
    "values": ""
}

column_selection_missing_values = {
    "name": "column_test",
    "data_type": "String",
    "column_type" : "Selection"
}

column_selection_values_multiple_types = {
    "name": "column_test",
    "data_type": "String",
    "column_type" : "Selection",
    "values": [
        "string",
        10,
        1.5,
        True
    ]
}

def test_parse_column_selection_valid():
    selection = parse_column_type(column_selection_valid)
    assert isinstance(selection, SchemaColumnSelection)
    assert vars(selection) == {"_name" : "column_test", "_values": ["value1","value2","value3"]}

def test_parse_column_selection_not_list():
    with pytest.raises(ValueError):
        parse_column_type(column_selection_not_list)

def test_parse_column_selection_empty_values():
    with pytest.raises(ValueError):
        parse_column_type(column_selection_empty_values)

def test_parse_column_selection_missing_values():
    with pytest.raises(ValueError):
        parse_column_type(column_selection_missing_values)

def test_parse_column_selection_values_multiple_types():
    with pytest.raises(ValueError):
        parse_column_type(column_selection_values_multiple_types)


# Integration tests for `parse_column_type()` for Expression columns

column_selection_valid_string = {
    "name": "column_test",
    "data_type": "String",
    "column_type" : "Selection",
    "values": [
        "value1",
        "value2",
        "value3"
    ]
}

column_selection_valid_int = {
    "name": "column_test",
    "data_type": "String",
    "column_type" : "Selection",
    "values": [
        10,
        15,
        20
    ]
}

column_selection_valid_float = {
    "name": "column_test",
    "data_type": "String",
    "column_type" : "Selection",
    "values": [
        5.1,
        2.4,
        6.9
    ]
}

column_selection_valid_bool = {
    "name": "column_test",
    "data_type": "String",
    "column_type" : "Selection",
    "values": [
        True,
        False,
        True
    ]
}

column_selection_valid_datetime = {
    "name": "column_test",
    "data_type": "String",
    "column_type" : "Selection",
    "values": [
        datetime.fromisoformat("2025-01-01T00:00:00"),
        datetime.fromisoformat("2025-06-15T12:30:30"),
        datetime.fromisoformat("2025-12-31T23:59:59")
    ]
}

column_selection_valid_date = {
    "name": "column_test",
    "data_type": "String",
    "column_type" : "Selection",
    "values": [
        date.fromisoformat("2025-01-01"),
        date.fromisoformat("2025-06-15"),
        date.fromisoformat("2025-12-31")
    ]
}

@pytest.mark.parametrize("input, result", [
    (column_selection_valid_string, {"_name" : "column_test", "_values" : ["value1","value2","value3"]}),
    (column_selection_valid_int, {"_name" : "column_test", "_values" : [10,15,20]}),
    (column_selection_valid_float, {"_name" : "column_test", "_values" : [5.1,2.4,6.9]}),
    (column_selection_valid_bool, {"_name" : "column_test", "_values" : [True, False, True]}),
    (column_selection_valid_datetime, {"_name" : "column_test", "_values" : [datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0),
                                                                             datetime(year=2025, month=6, day=15, hour=12, minute=30, second=30),
                                                                             datetime(year=2025, month=12, day=31, hour=23, minute=59, second=59)]}),
    (column_selection_valid_date, {"_name" : "column_test", "_values" : [date(year=2025, month=1, day=1),
                                                                         date(year=2025, month=6, day=15),
                                                                         date(year=2025, month=12, day=31)]})

])
def test_parse_column_selection_valid_par_val(input, result):
    column = parse_column_type(input)
    assert isinstance(column, SchemaColumnSelection)
    assert vars(column) == result


column_selection_multiple_types_1 = {
    "name": "column_test",
    "data_type": "String",
    "column_type" : "Selection",
    "values": [
        "value1",
        5.1,
        True
    ]
}

column_selection_multiple_types_2 = {
    "name": "column_test",
    "data_type": "String",
    "column_type" : "Selection",
    "values": [
        False,
        datetime.fromisoformat("2025-12-31T23:59:59"),
        10
    ]
}

@pytest.mark.parametrize("input",[
    (column_selection_multiple_types_1),
    (column_selection_multiple_types_2)
])
def test_parse_column_type_selection_multiple_types_values(input):
    with pytest.raises(ValueError):
        parse_column_type(input)