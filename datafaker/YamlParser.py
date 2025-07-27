import yaml
from datafaker.Schema import Schema, SchemaTable, parse_column_type
        
def parse_schema_from_file(file):
    with open(file, "r") as file:
        schema_defs = yaml.safe_load(file)
    
    tables = []
    for tbl in schema_defs.get("tables", []):

        columns = [ parse_column_type(col) for col in tbl.get("columns", []) ]

        table = SchemaTable(
            name = tbl["name"],
            rows = tbl["rows"],
            columns = columns
        )

        tables.append(table)
    
    return Schema(tables = tables)