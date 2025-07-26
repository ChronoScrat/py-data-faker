import yaml
import datetime as dt
from datetime import datetime

def import_file(file):
    with open(file, "r") as file:
        schema_defs = yaml.safe_load(file)
    return schema_defs
        
def validate_schema(schema):
    return True

def parse_schema_from_file(file):
    schema_defs = import_file(file)
    val_schema = validate_schema(schema_defs)
    return val_schema