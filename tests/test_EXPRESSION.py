# Testing suite for column type: EXPRESSION

from datafaker.SchemaParser import *
import pytest

# Unit tests for SchemaColumnExpression

def test_SchemaColumnExpression_valid():
    column = SchemaColumnExpression(name = "column_name", expression="rand()")
    assert isinstance(column, SchemaColumnExpression)
    assert vars(column) == {"_name" : "column_name", "_expression" : "rand()"}

@pytest.mark.parametrize("input",[
    (10),
    (5.1),
    (True),
    (datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0))
])
def test_SchemaColumnExpression_non_str(input):
    with pytest.raises(TypeError):
        SchemaColumnExpression(name = "column_test", expression = input)

def test_SchemaColumnExpression_missin_expr():
    with pytest.raises(TypeError):
        SchemaColumnExpression(name = "column_name")

# Unit tests for `parse_column_type()` for Expression Columns

column_expression_valid = {
    "name": "column_test",
    "data_type": "Double",
    "column_type" : "Expression",
    "expression" : "round(lit(3.1415), lit(2))"
}

column_expression_missing_expr = {
    "name": "column_test",
    "data_type": "Double",
    "column_type" : "Expression"
}

def test_parse_column_expression_valid():
    expression = parse_column_type(column_expression_valid)
    assert isinstance(expression, SchemaColumnExpression)
    assert vars(expression) == {"_name" : "column_test", "_expression" : "round(lit(3.1415), lit(2))"}

def test_parse_column_expression_missing_expr():
    with pytest.raises(ValueError):
        parse_column_type(column_expression_missing_expr)

# Integration tests for `parse_column_type()` for Expression columns

column_expression_non_str_int = {
    "name": "column_test",
    "data_type": "Double",
    "column_type" : "Expression",
    "expression" : 500
}

column_expression_non_str_float = {
    "name": "column_test",
    "data_type": "Double",
    "column_type" : "Expression",
    "expression" : 5.1
}

column_expression_non_str_bool = {
    "name": "column_test",
    "data_type": "Double",
    "column_type" : "Expression",
    "expression" : True
}

column_expression_non_str_time = {
    "name": "column_test",
    "data_type": "Double",
    "column_type" : "Expression",
    "expression" : datetime.fromisoformat("2025-01-01T00:00:00Z")
}

column_expression_expr_list_str = {
    "name": "column_test",
    "data_type": "Double",
    "column_type" : "Expression",
    "expression" : ["rand()", "lit(2)"]
}

column_expression_expr_whitespace = {
    "name": "column_test",
    "data_type": "Double",
    "column_type" : "Expression",
    "expression" : "    "
}

@pytest.mark.parametrize("input",[
    (column_expression_non_str_int),
    (column_expression_non_str_float),
    (column_expression_non_str_bool),
    (column_expression_non_str_time)
])
def test_parse_column_expression_non_str_expr(input):
    with pytest.raises(TypeError):
        parse_column_type(input)

def test_parse_column_expression_expr_list_str():
    with pytest.raises(TypeError):
        parse_column_type(column_expression_expr_list_str)

# TODO Possibly treat whitespace-only expressions as empty
def test_parse_column_expression_expr_whitespace():
    column = parse_column_type(column_expression_expr_whitespace)
    assert isinstance(column, SchemaColumnExpression)