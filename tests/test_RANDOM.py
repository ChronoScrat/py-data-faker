# Testing suite for column type: RANDOM

from datafaker.SchemaParser import *
import pytest

# Unit tests for SchemaColumnRandom*

# We will check the Factory class, as it is the one used to determine 
# which of the other `SchemaColumnRandom*` classes will be used.

from datetime import datetime, date

def test_SchemaColumnRandomFactory_bool_valid():
    bool_c = SchemaColumnRandomFactory.create(name = "column_name")
    assert isinstance(bool_c, SchemaColumnRandomBoolean)

def test_SchemaColumnRandomFactory_bool_min():
    with pytest.raises(TypeError):
        SchemaColumnRandomFactory.create(name = "column_name",
                                         min = 10)
    
def test_SchemaColumnRandomFactory_bool_max():
    with pytest.raises(TypeError):
        SchemaColumnRandomFactory.create(name = "column_name",
                                         max = 10)

def test_SchemaColumnRandomFactory_invalid_min():
    with pytest.raises(TypeError):
        SchemaColumnRandomFactory.create(name = "column_name",
                                         min = "string",
                                         max = 10)

def test_SchemaColumnRandomFactory_invalid_max():
    with pytest.raises(TypeError):
        SchemaColumnRandomFactory.create(name = "column_name",
                                         min = 1,
                                         max = "string")

def test_SchemaColumnRandomFactory_numeric_int_valid():
    numeric = SchemaColumnRandomFactory.create(name = "column_name", min = 1, max = 10)
    assert isinstance(numeric, SchemaColumnRandomNumeric)

def test_SchemaColumnRandomFactory_numeric_double_valid():
    numeric = SchemaColumnRandomFactory.create(name = "column_name", min = 1.5, max = 1.2)
    assert isinstance(numeric, SchemaColumnRandomNumeric)

def test_SchemaColumnRandomFactory_numeric_mismatch():
    with pytest.raises(TypeError):
        SchemaColumnRandomFactory.create(name="column_name",
                                         min=1,
                                         max = 10.5)
        
def test_SchemaColumnRandomFactory_numeric_missing_min():
    with pytest.raises(TypeError):
        SchemaColumnRandomFactory.create(name="column_name",
                                         max = 10.5)

def test_SchemaColumnRandomFactory_numeric_missing_max():
    with pytest.raises(TypeError):
        SchemaColumnRandomFactory.create(name="column_name",
                                         min=1)

def test_SchemaColumnRandomFactory_timestamp_valid():
    timestamp_c = SchemaColumnRandomFactory.create(name = "column_test",
                                                   min = datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0),
                                                   max = datetime(year=2025, month=12, day=31, hour=23, minute=59, second=59))
    assert isinstance(timestamp_c, SchemaColumnRandomTimestamp)

def test_SchemaColumnRandomFactory_timestamp_invalid_max():
    with pytest.raises(TypeError):
        SchemaColumnRandomFactory.create(name = "column_test",
                                         min = datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0),
                                         max = 10)

def test_SchemaColumnRandomFactory_timestamp_missing_max():
    with pytest.raises(TypeError):
        SchemaColumnRandomFactory.create(name = "column_test",
                                         min = datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0))
        
def test_SchemaColumnRandomFactory_timestamp_invalid_min():
    with pytest.raises(TypeError):
        SchemaColumnRandomFactory.create(name = "column_test",
                                         min = "string",
                                         max = datetime(year=2025, month=12, day=31, hour=23, minute=59, second=59))

def test_SchemaColumnRandomFactory_timestamp_missing_min():
    with pytest.raises(TypeError):
        SchemaColumnRandomFactory.create(name = "column_test",
                                         max = datetime(year=2025, month=12, day=31, hour=23, minute=59, second=59))

def test_SchemaColumnRandomFactory_date_valid():
    timestamp_c = SchemaColumnRandomFactory.create(name = "column_test",
                                                   min = date(year=2025, month=1, day=1),
                                                   max = date(year=2025, month=12, day=31))
    assert isinstance(timestamp_c, SchemaColumnRandomDate)

def test_SchemaColumnRandomFactory_date_invalid_min():
    with pytest.raises(TypeError):
        SchemaColumnRandomFactory.create(name = "column_test",
                                         min = "string",
                                         max = date(year=2025, month=12, day=31))

def test_SchemaColumnRandomFactory_date_missing_min():
    with pytest.raises(TypeError):
        SchemaColumnRandomFactory.create(name = "column_test",
                                         min = "string")

def test_SchemaColumnRandomFactory_date_invalid_max():
    with pytest.raises(TypeError):
        SchemaColumnRandomFactory.create(name = "column_test",
                                         min = date(year=2025, month=1, day=1),
                                         max = "string")

def test_SchemaColumnRandomFactory_date_missing_max():
    with pytest.raises(TypeError):
        SchemaColumnRandomFactory.create(name = "column_test",
                                         min = date(year=2025, month=1, day=1),
                                         max = "string")

def test_SchemaColumnRandomFactory_date_timestamp_mismatch():
    with pytest.raises(TypeError):
        SchemaColumnRandomFactory.create(name = "column_test",
                                         min = date(year=2025, month=1, day=1),
                                         max = datetime(year=2025, month=12, day=31, hour=23, minute=59, second=59))


# Unit tests for `parse_column_type()` for Random Columns

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

column_random_valid_falsy = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Random",
    "min": 0,
    "max" : 0
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
    random = parse_column_type(column_random_valid)
    assert isinstance(random, SchemaColumnRandom)

def test_parse_column_random_falsy_zero():
    falsy_z = parse_column_type(column_random_valid_falsy)
    assert isinstance(falsy_z, SchemaColumnRandom)
    assert vars(falsy_z) == {"_name" : "column_test", "_min" : 0, "_max" : 0}


# Integration tests for `parse_column_type()` for Random columns

column_random_int_valid = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Random",
    "min": 5,
    "max" : 10
}

column_random_float_valid = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Random",
    "min": 2.0,
    "max" : 10.0
}

column_random_timestamp_valid = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Random",
    "min": datetime.fromisoformat("2025-01-01T00:00:00Z"),
    "max" : datetime.fromisoformat("2025-01-31T23:59:59Z")
}

column_random_date_valid = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Random",
    "min": date.fromisoformat("2025-01-01"),
    "max" : date.fromisoformat("2025-01-31")
}

column_random_boolean_valid = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Random",
    "min": None,
    "max" : None
}

def test_parse_column_random_int_valid():
    column = parse_column_type(column_random_int_valid)
    assert isinstance(column, SchemaColumnRandomNumeric)
    assert vars(column) == {"_name" : "column_test",
                            "_min" : 5,
                            "_max" : 10}

def test_parse_column_random_float_valid():
    column = parse_column_type(column_random_float_valid)
    assert isinstance(column, SchemaColumnRandomNumeric)
    assert vars(column) == {"_name" : "column_test",
                            "_min" : 2.0,
                            "_max" : 10.0}

def test_parse_column_random_timestamp_valid():
    column = parse_column_type(column_random_timestamp_valid)
    assert isinstance(column, SchemaColumnRandomTimestamp)

def test_parse_column_random_date_valid():
    column = parse_column_type(column_random_date_valid)
    assert isinstance(column, SchemaColumnRandomDate)

def test_parse_column_random_bool_valid():
    column = parse_column_type(column_random_boolean_valid)
    assert isinstance(column, SchemaColumnRandomBoolean)


column_random_min_non_numeric = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Random",
    "min": "string",
    "max" : 10
}

column_random_max_non_numeric = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Random",
    "min": 1,
    "max" : "string"
}

column_random_min_max_mismatch_num = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Random",
    "min": 2.0,
    "max" : 10
}

column_random_min_max_mismatch_num_2 = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Random",
    "min": 2,
    "max" : 10.0
}

column_random_min_max_mismatch_datetime = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Random",
    "min": datetime.fromisoformat("2025-01-01T00:00:00Z"),
    "max" : date.fromisoformat("2025-01-31")
}

column_random_min_max_mismatch_date = {
    "name": "column_test",
    "data_type": "Int",
    "column_type" : "Random",
    "min": date.fromisoformat("2025-01-01"),
    "max" : datetime.fromisoformat("2025-01-31T23:59:59Z")
}


def test_parse_column_random_min_non_numeric():
    with pytest.raises(TypeError):
        parse_column_type(column_random_min_non_numeric)

def test_parse_column_random_max_non_numeric():
    with pytest.raises(TypeError):
        parse_column_type(column_random_max_non_numeric)

def test_parse_column_random_min_max_mismatch_num():
    with pytest.raises(TypeError):
        parse_column_type(column_random_min_max_mismatch_num)

def test_parse_column_random_min_max_mismatch_num_2():
    with pytest.raises(TypeError):
        parse_column_type(column_random_min_max_mismatch_num_2)

def test_parse_column_random_min_max_mismatch_datetime():
    with pytest.raises(TypeError):
        parse_column_type(column_random_min_max_mismatch_datetime)

def test_parse_column_random_min_max_mismatch_date():
    with pytest.raises(TypeError):
        parse_column_type(column_random_min_max_mismatch_date)