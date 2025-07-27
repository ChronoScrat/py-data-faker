# Schemas file

# This file holds the Schema classes for all supported column types in `data-faker`,
# as well as the schema classes for the tables and for the resulting database. In the
# original, this was spread out across multiple files. I chose to combine them into
# a single file because of python shenenigans.

# Code is adpted from dunnhumby's original implementation in Scala.
# Please check: https://github.com/dunnhumby/data-faker


from dataclasses import dataclass
from typing import List, Optional, Generic, TypeVar, Union, Any
import builtins
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, date

import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql.types import IntegerType, DoubleType, StringType, DateType, TimestampType


# Schema Class: this is the master schema class, that holds a list of different tables.
# This is passed to DataGenerator to create all the tables with fake data.

@dataclass
class Schema:
    tables: List['SchemaTable']

# SchemaTable Class: this is the schema class for a single table. It holds the name, number of rows
# and all the columns for the table.

@dataclass
class SchemaTable:
    name: str
    rows: int
    columns: List['SchemaColumn']
    partitions: Optional[List['str']]

# SchemaColumn class: this is an abstract class (hence the use of ABC and @abstractmethod). The details
# are actually implemented by the subclasses as needed.

class SchemaColumn(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def column(self, rowID: Optional[Column] = None) -> Column:
        pass


# Expression Schema Column Subclass:
# This column allows the user to pass a SQL expression to the DataGenerator

class SchemaColumnExpression(SchemaColumn):
    def __init__(self, name: str, expression: str):
        if not isinstance(expression, str):
            raise TypeError(f"Expected string for expression in column {name}, got {type(expression).__name__}")
        self._name = name
        self._expression = expression
    
    @property
    def name(self) -> str:
        return self._name
    
    def column(self, rowID: Optional[Column] = None) -> Column:
        return F.expr(self._expression)


# Fixed Schema Column Class:
# This column allows the user to pass a single value to the DataGenerator

T = TypeVar('T')

class SchemaColumnFixed(SchemaColumn):
    def __init__(self, name: str, value: T):
        self._name = name
        self._value = value

    @property
    def name(self) -> str:
        return self._name
    
    def column(self, rowID: Optional[Column] = None) -> Column:
        return F.lit(self._value)
    

# Sequential Schema Column Class:
# This column allows values to be generated sequentially. This is implemented
# by using the ephemeral `rowID` column. The actual class is merely a placeholder,
# the actual implementation depends on the column type. A "Factory" is then used
# to decide which class will actually be implemented.

class SchemaColumnSequential(SchemaColumn):
    pass

class SchemaColumnSequentialNumeric(SchemaColumnSequential):
    def __init__(self, name: str, start: Union[int, float], step: Union[int, float]):
        self._name = name
        self._start = start
        self._step = step
    
    @property
    def name(self) -> str:
        return self._name
    
    def column(self, rowID: Optional[Column] = None) -> Column:
        if rowID is None:
            rowID = F.monotonically_increasing_id()
        return ((rowID * F.lit(self._step)) + F.lit(self._start))

class SchemaColumnSequentialTimestamp(SchemaColumnSequential):
    def __init__(self, name: str, start: datetime, step_seconds: int):
        self._name = name
        self._start = int(start.timestamp())
        self._step_seconds = step_seconds
    
    @property
    def name(self) -> str:
        return self._name
    
    def column(self, rowID: Optional[Column] = None) -> Column:
        if rowID is None:
            rowID = F.monotonically_increasing_id()
        in_seconds = ((rowID * F.lit(self._step_seconds)) + F.lit(self._start))
        in_seconds_unix = F.from_unixtime(in_seconds)
        return F.to_utc_timestamp(in_seconds_unix, "UTC")


class SchemaColumnSequentialDate(SchemaColumnSequential):
    def __init__(self, name: str, start: date, step_days: int):
        self._name = name
        self._timestamp = SchemaColumnSequentialTimestamp(
            name,
            datetime.combine(start, datetime.min.time()),
            step_days * 86400
        )
    
    @property
    def name(self) -> str:
        return self._name
    
    def column(self, rowID: Optional[Column] = None) -> Column:
        return F.to_date(self._timestamp.column(rowID))

# Factory: We use this to determine what class to use for SchemaColumn based on the
# type of both `start` and `step`
class SchemaColumnSequentialFactory:
    @staticmethod
    def create(name: str, start, step) -> SchemaColumn:
        if isinstance(start, (int, float)) and isinstance(step, (int, float)):
            return SchemaColumnSequentialNumeric(name, start, step)
        elif isinstance(start, datetime) and isinstance(step, int):
            return SchemaColumnSequentialTimestamp(name, start, step)
        elif isinstance(start, date) and isinstance(step, int):
            return SchemaColumnSequentialDate(name, start, step)
        else:
            raise TypeError(f"Unsupported start/step types: {type(start)}, {type(step)}")
        

# Random Schema Column Class:
# This column allows values to be generated randomly. The actual class is merely 
# a placeholder, the actual implementation depends on the column type. 
# A "Factory" is then used to decide which class will actually be implemented.

class SchemaColumnRandom(SchemaColumn):
    pass


class SchemaColumnRandomNumeric(SchemaColumnRandom):
    def __init__(self, name: str,min: Union[int, float], max: Union[int, float]):
        self._name = name
        self._min = min
        self._max = max
    
    @property
    def name(self) -> str:
        return self._name
    
    def column(self, rowID: Optional[Column] = None) -> Column:
        base = F.rand() * (self._max - self._min) + self._min

        if ( isinstance(self._min, int) & isinstance(self._max, int) ):
            return F.round(base, 0).cast(IntegerType())
        elif ( isinstance(self._min, float) & isinstance(self._max, float) ):
            return F.round(base, 3).cast(DoubleType())
        else:
            raise TypeError("Unsupported numeric types for random numeric column")

class SchemaColumnRandomTimestamp(SchemaColumnRandom):
    def __init__(self, name:str, min: datetime, max: datetime):
        self._name = name
        self._min = int(min.timestamp())
        self._max = int(max.timestamp())
    
    @property
    def name(self) -> str:
        return self._name
    
    def column(self, rowID: Optional[Column] = None) -> Column:
        base = F.rand() * (self._max - self._min) + self._min
        return F.to_utc_timestamp(F.from_unixtime(base), "UTC")
    
class SchemaColumnRandomDate(SchemaColumnRandom):
    def __init__(self, name: str, min: date, max: date):
        self._name = name
        self._timestamp = SchemaColumnRandomTimestamp(
            name,
            min = datetime.combine(min, datetime.min.time()),
            max = datetime.combine(max + timedelta(days=1), datetime.min.time())
        )
    
    @property
    def name(self) -> str:
        return self._name
    
    def column(self, rowID: Optional[Column] = None) -> Column:
        return F.to_date(self._timestamp.column(rowID))


class SchemaColumnRandomBoolean(SchemaColumnRandom):
    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        return self._name
    
    def column(self, rowID: Optional[Column] = None) -> Column:
        return F.rand() < 0.5


class SchemaColumnRandomFactory:
    @staticmethod
    def create(name: str, min: Any = None, max: Any = None):
        if (min == None) & (max == None):
            return SchemaColumnRandomBoolean(name)


        if ( isinstance(min, int) & isinstance(max, int) ):
            return SchemaColumnRandomNumeric(name, min, max)
        elif ( isinstance(min, float) & isinstance(max, float) ):
            return SchemaColumnRandomNumeric(name, min, max)
        elif ( isinstance(min, date) & isinstance(max, date) ):
            return SchemaColumnRandomDate(name, min, max)
        elif ( isinstance(min, datetime) & isinstance(max, datetime) ):
            return SchemaColumnRandomTimestamp(name, min, max)
        else:
            raise TypeError(f"Unsupported types for random column: min={type(min)}, max={type(max)}")


# Selection Schema Column Class
# This column allows the user to supply a list of possible valued to
# the DataGenerator, and one will be randomly selected.

class SchemaColumnSelection(SchemaColumn):
    def __init__(self, name: str, values: List[Any]):
        self._name = name
        self._values = values
    
    @property
    def name(self) -> str:
        return self._name
    
    def column(self, rowID: Optional[Column] = None) -> Column:
        values = self._values
        num_val = len(values)

        def pick(index: int):
            return values[index % num_val]
        
        first_val = values[0]
        first_val_type = type(first_val)
        
        match first_val_type:
            case builtins.int:
                return_type = IntegerType()
            case builtins.float:
                return_type = DoubleType()
            case builtins.str:
                return_type = StringType()
            case datetime.date:
                return_type = DateType()
            case datetime.datetime:
                return_type = TimestampType()
            case _:
                raise TypeError(f"Unsupported value type: {type(first_val)}")
        
        to_udf_selection = F.udf(pick, return_type)
        return to_udf_selection((F.rand() * num_val).cast("int"))
