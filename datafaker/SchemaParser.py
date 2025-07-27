import yaml
from datafaker.Schemas import *


# Column Parsing
# The following Class and method are used to define what Schema Class will
# be used for a given column defined in the schema file.
class SchemaColumnType:
    FIXED = "Fixed"
    RANDOM = "Random"
    SELECTION = "Selection"
    SEQUENTIAL = "Sequential"
    EXPRESSION = "Expression"


def parse_column_type(column: dict) -> SchemaColumn:
    column_name = column.get("name")
    column_type = column.get("column_type")

    if not column_name:
        raise ValueError("Column missing name")
    
    if not column_type:
        raise ValueError("Missing column_type")

    match column_type:

        case SchemaColumnType.FIXED:
            column_value = column.get("value")
            if not column_value and column_value != 0:
                raise ValueError(f"Column {column_name} of type {column_type} is missing value")
            else:
                return SchemaColumnFixed(name = column_name, value = column_value)
        
        case SchemaColumnType.EXPRESSION:
            column_expr = column.get("expression")
            if not column_expr:
                raise ValueError(f"Column {column_name} of type {column_type} is missing expression")
            else:
                return SchemaColumnExpression(name = column_name, expression = column_expr)
            
        case SchemaColumnType.SELECTION:
            column_values = column.get("values")
            if not column_values:
                raise ValueError(f"Column {column_name} of type {column_type} is missing values")
            else:
                return SchemaColumnSelection(name = column_name, values = column_values)
            
        case SchemaColumnType.SEQUENTIAL:
            column_start = column.get("start")
            column_step = column.get("step")
            if not column_start and column_start != 0:
                raise ValueError(f"Column {column_name} of type {column_type} is missing start")
            elif not column_step and column_step != 0:
                raise ValueError(f"Column {column_name} of type {column_type} is missing step")
            else:
                return SchemaColumnSequentialFactory.create(name = column_name, start = column_start, step = column_step)
        
        case SchemaColumnType.RANDOM:
            column_min = column.get("min")
            column_max = column.get("max")
            if not column_min and column_min != 0:
                raise ValueError(f"Column {column_name} of type {column_type} is missing min")
            elif not column_max and column_max != 0:
                raise ValueError(f"Column {column_name} of type {column_type} is missing max")
            else:
                return SchemaColumnRandomFactory.create(name = column_name, min = column_min, max = column_max)

# Parse Schema from file
# This method recieves the path to a file (the schema file) and returns
# our Schema object, which contains all tables and columns to be generated.

def parse_schema_from_file(file):
    with open(file, "r") as file:
        schema_defs = yaml.safe_load(file)
    
    tables = []
    for tbl in schema_defs.get("tables", []):

        columns = [ parse_column_type(col) for col in tbl.get("columns", []) ]
        partitions = tbl.get("partitions")

        table = SchemaTable(
            name = tbl["name"],
            rows = tbl["rows"],
            columns = columns,
            partitions = partitions
        )

        tables.append(table)
    
    return Schema(tables = tables)