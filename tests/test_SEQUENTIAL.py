# Testing suite for column type: SEQUENTIAL

from datafaker.SchemaParser import *
import pytest


# Unit tests for SchemaColumnSequential*

# We will check the Factory class, as it is the one used to determine 
# which of the other `SchemaColumnSequential*` classes will be used.

from datetime import datetime, date

def test_SchemaColumnSequentialFactory_numeric_valid():
    numeric = SchemaColumnSequentialFactory.create(name="column_test", start=5, step=1.5)
    assert isinstance(numeric, SchemaColumnSequentialNumeric)

def test_SchemaColumnSequentialFactory_timestamp_valid():
    timestamp = SchemaColumnSequentialFactory.create(name="column_test", 
                                                     start=datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0),
                                                     step=60)
    assert isinstance(timestamp, SchemaColumnSequentialTimestamp)

def test_SchemaColumnSequentialFactory_date_valid():
    date_col = SchemaColumnSequentialFactory.create(name="column_test",
                                                    start=date(year=2025, month=1, day=1),
                                                    step=1)
    assert isinstance(date_col, SchemaColumnSequentialDate)

def test_SchemaColumnSequentialFactory_start_non_numeric():
    with pytest.raises(TypeError):
        SchemaColumnSequentialFactory.create(name = "column_test",
                                             start = "string",
                                             step = 1)

def test_SchemaColumnSequentialFactory_step_non_numeric():
    with pytest.raises(TypeError):
        SchemaColumnSequentialFactory.create(name = "column_test",
                                             start = 0,
                                             step = "string")

def test_SchemaColumnSequentialFactory_datetime_step_non_int():
    with pytest.raises(TypeError):
        SchemaColumnSequentialFactory.create(name = "column_test",
                                             start = datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0),
                                             step = 1.5)

def test_SchemaColumnSequentialFactory_date_step_non_int():
    with pytest.raises(TypeError):
        SchemaColumnSequentialFactory.create(name = "column_test",
                                             start = date(year=2025, month=1, day=1),
                                             step = 1.5)


# Unit tests for `parse_column_type()` for Sequential Columns

column_sequential_valid = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Sequential",
    "start": 10,
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
    "start": 5
}

column_sequential_missing_both = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Sequential"
}

column_sequential_valid_falsy = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Sequential",
    "start": 0,
    "step" : 0
}

def test_parse_column_sequential_valid():
    sequential = parse_column_type(column_sequential_valid)
    assert isinstance(sequential, SchemaColumnSequential)
    assert vars(sequential) == {"_name" : "column_test", "_start": 10, "_step" : 1}

def test_parse_column_sequential_missing_start():
    with pytest.raises(ValueError):
        parse_column_type(column_sequential_missing_start)

def test_parse_column_sequential_missing_step():
    with pytest.raises(ValueError):
        parse_column_type(column_sequential_missing_step)

def test_parse_column_sequential_missing_both():
    with pytest.raises(ValueError):
        parse_column_type(column_sequential_missing_both)

def test_parse_column_sequential_falsy_zero():
    falsy_z = parse_column_type(column_sequential_valid_falsy)
    assert isinstance(falsy_z, SchemaColumnSequential)
    assert vars(falsy_z) == {"_name" : "column_test", "_start" : 0, "_step" : 0}


# Integration tests for `parse_column_type()` for Sequential columns

column_sequential_numeric_valid = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Sequential",
    "start": 10,
    "step" : 1
}

column_sequential_timestamp_valid = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Sequential",
    "start": datetime.fromisoformat("2025-01-01T00:00:00Z"),
    "step" : 60
}

column_sequential_date_valid = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Sequential",
    "start": date.fromisoformat("2025-01-01"),
    "step" : 1
}


def test_parse_column_sequential_numeric_valid():
    numeric = parse_column_type(column_sequential_numeric_valid)
    assert isinstance(numeric, SchemaColumnSequentialNumeric)
    assert vars(numeric) == {"_name" : "column_test",
                             "_start" : 10,
                             "_step" : 1}

def test_parse_column_sequential_timestamp_valid():
    timestamp = parse_column_type(column_sequential_timestamp_valid)
    assert isinstance(timestamp, SchemaColumnSequentialTimestamp)
    assert vars(timestamp) == {"_name" : "column_test", 
                             "_start" : int(datetime.fromisoformat("2025-01-01T00:00:00Z").timestamp()),
                             "_step_seconds" : 60}

# TODO When we convert SchemaColumn* to `@dataclass`, add assertion to values
def test_parse_column_sequential_date_valid():
    date_col = parse_column_type(column_sequential_date_valid)
    assert isinstance(date_col, SchemaColumnSequentialDate)


column_sequential_start_non_numeric = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Sequential",
    "start": "string",
    "step" : 1
}

column_sequential_step_non_numeric = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Sequential",
    "start": 0,
    "step" : "string"
}

column_sequential_datetime_step_non_int = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Sequential",
    "start": "2025-01-01T00:00:00Z",
    "step" : 1.5
}

column_sequential_date_step_non_int = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Sequential",
    "start": "2025-01-01",
    "step" : 1.5
}

def test_parse_column_sequential_start_non_numeric():
    with pytest.raises(TypeError):
        parse_column_type(column_sequential_start_non_numeric)

def test_parse_column_sequential_step_non_numeric():
    with pytest.raises(TypeError):
        parse_column_type(column_sequential_step_non_numeric)

def test_parse_column_sequential_datetime_step_non_int():
    with pytest.raises(TypeError):
        parse_column_type(column_sequential_datetime_step_non_int)

def test_parse_column_sequential_date_step_non_int():
    with pytest.raises(TypeError):
        parse_column_type(column_sequential_date_step_non_int)
