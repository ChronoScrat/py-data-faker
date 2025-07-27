# Testing suite for `parse_colum_type()` from SchemaParser.py
# Column Type: EXPRESSION

from datafaker.SchemaParser import *
import pytest


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