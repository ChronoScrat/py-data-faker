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
from pyspark.sql.types import IntegerType, DoubleType, StringType, DateType, TimestampType, BooleanType


# Schema Class: this is the master schema class, that holds a list of different tables.
# This is passed to DataGenerator to create all the tables with fake data.

@dataclass
class Schema:
    """The Schema class represents a full database schema defined in a schema file. It holds
    a list of tables to be generated and written in Spark.

    Args:
        tables (list of SchemaTable): A list of SchemaTable objects
    """
    tables: List['SchemaTable']

# SchemaTable Class: this is the schema class for a single table. It holds the name, number of rows
# and all the columns for the table.

@dataclass
class SchemaTable:
    """The SchemaTable class represents a table in a database. It holds the required information for the table
    such as the name, number of rows, columns and (optionally) partitions.

    Args:
        name (str): The table name. It must contain only characters allowed in Spark table names
        rows (int): Number of rows in the table
        columns (list of SchemaColumn): A list of SchemaColumn objects which will define all columns in the table
        partitions (list of str, optional): An optional list of column names which will be used as partitions in the table
    """
    name: str
    rows: int
    columns: List['SchemaColumn']
    partitions: Optional[List['str']]

# SchemaColumn class: this is an abstract class (hence the use of ABC and @abstractmethod). The details
# are actually implemented by the subclasses as needed.

class SchemaColumn(ABC):
    """The SchemaColumn class represents a column in a table. This class itself is an Abstract Class,
    and the actual implementations depends on the type of column. They are implemented in the form of
    `SchemaColumn<TYPE>`. Each column has a name and a method called `column`, which is an instance of
    `spark.sql.Column`.
    """
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
    """An extension of SchemaColumn that represents a column of the type `Expression` in the
    user-provided schema. It takes a name and a SQL expression which will be passed onto Spark
    when the data is generated.

    Args:
        name (str): Column name
        expression (str): A Spark SQL expression
    """
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
    """An extension of SchemaColumn that represents a column of type `Fixed` in the user-provided
    schema. It takes a name and a value which will be passed onto Spark and replicated across all
    rows of the table.

    Args:
        name (str): Column name
        value (Any): The fixed value which will be passed to Spark.
    """
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
    """An extension of SchemaColumn that represents a column of type `Sequential` in the user-provided
    schema. This class itself is merely a placeholder, and the actual implementation depends on the
    type of the data which will be generated.
    A 'Factory Class' `SchemaColumnSequentialFactory` is used to determine which implementation will be
    used.
    """
    pass

class SchemaColumnSequentialNumeric(SchemaColumnSequential):
    """An extension of SchemaColumnSequential that represents a Sequential column holding numeric data.
    It requires a name, a starting point (inclusive) and an amount to step in each interaction. The final
    value in each rows depends on the ID (number) of the row.

    Args:
        name (str): Column name
        start (int or float): A value to serve as the starting point of the sequence
        step (int or float): A value to increment the starting point depending on the row
    """
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
    """An extension of SchemaColumnSequential that represents a Sequential column holding timestamp
    (datetime) data. It requires a starting point passed as a datetime object and an amount of seconds
    to increment in each row.

    Args:
        name (str): Column name
        start (datetime): A datetime object which will serve as the starting point of the sequence
        step_seconds (int): A value to increment the starting point in seconds.
    """
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
    """An extension of SchemaColumnSequential that represents a Sequential column holding date
    objects. It requires a starting point passed as a date object and an amount of days to increment
    in each row.

    Args:
        name (str): Column name
        start (date): A date object which will serve as the starting point of the sequence
        step_days (int): A value to increment the starting point in days
    """
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
    """This factory class implements the logic for choosing which `SchemaColumnSequential*`
    class will be used when the user provides a Sequential column in the schema. The choosing
    logic is implemented by checking the types of both `start` and `step`.


    Example:
        If we pass the values 10 and 5 to start and step respectively, our factory
        will create an instance of SchemaColumnNumeric
        >>> column = (
        ...     SchemaColumnSequentialFactory
        ...         .create(name = 'my_sequence', start = 10, step = 5)
        ... )

    """
    @staticmethod
    def create(name: str, start, step) -> SchemaColumn:
        """Creates an instance of `SchemaColumnSequential*` based on the type of the values
        passed to `start` and `step`. 

        Please note that although `step` may be an int or a float, the latter cannot be used if `start`
        is a date or datetime object.

        Args:
            name (str): Column name
            start (int or float or datetime or date): A value which will serve as the starting point for the sequence
            step (int or float): A value to increment the sequence in each row.
        """
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
    """An extension of SchemaColumn that represents a column of type `Random` in the user-provided
    schema. This class itself is merely a placeholder, and the actual implementation depends on the
    type of the data which will be generated.
    A 'Factory Class' `SchemaColumnRandomFactory` is used to determine which implementation will be
    used.
    """
    pass


class SchemaColumnRandomNumeric(SchemaColumnRandom):
    """An extension of SchemaColumnRandom that represents a Random column of numeric type data. It
    requires a minimum and maximum value, and generates numeric values inbetween them. This class
    allows for either integer or floats in `min` and `max`, but the types must match for both
    arguments.
    The Spark column is creater either as an Integer or a as Double type.

    Args:
        name (str): Column name
        min (int or float): The lower bound in the random data generation
        max (int or float): The upper bound in the random data generation
    """
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
    """An extension of SchemaColumnRandom that represents a Random column of datetime/timestamp
    type data. It requires a minimum and a maximum value, and both must be passed as datetime
    objects.

    Args:
        name (str): Column name
        min (datetime): The lower bound in the random data generation as a datetime object
        max (datetime): The upper bound in the random data generation as a datetime object
    """
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
    """An extension of SchemaColumnRandom that represents a Random column of date-type data and
    objects. It requires a minimum and a maximum value, and both must be passed as date
    objects.

    Args:
        name (str): Column name
        min (datetime): The lower bound in the random data generation as a date object
        max (datetime): The upper bound in the random data generation as a date object
    """
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
    """An extension of SchemaColumnRandom that represents a Random column of boolean type
    data. It requires no minimum or maximum value.

    Args:
        name (str): Column name
    """
    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        return self._name
    
    def column(self, rowID: Optional[Column] = None) -> Column:
        return F.rand() < 0.5


class SchemaColumnRandomFactory:
    """A Factory class that chooses the which of the `SchemaColumnRandom*` classes
    will be used when the user provides a Random column in the schema file. The logic
    is implemented by checking the type of the (optionally) provided `min` and `max`.
    """
    @staticmethod
    def create(name: str, min: Any = None, max: Any = None):
        """Creates an instance of `SchemaColumnRandom*` based on the type of the values
        passed to `min` and `max`. 

        The types of `min` and `max` must match (including if they are None).

        Args:
            name (str): Column name
            min (int or float or datetime or date or None): The lower bound in the random data generation
            max (int or float or datetime or date or None): The upper bound in the random data generation
        """
        if (min == None) & (max == None):
            return SchemaColumnRandomBoolean(name)


        if ( isinstance(min, int) & isinstance(max, int) ):
            return SchemaColumnRandomNumeric(name, min, max)
        elif ( isinstance(min, float) & isinstance(max, float) ):
            return SchemaColumnRandomNumeric(name, min, max)
        elif ( isinstance(min, datetime) & isinstance(max, datetime) ):
            return SchemaColumnRandomTimestamp(name, min, max)
        elif ( isinstance(min, date) & isinstance(max, date) & (not isinstance(max, datetime)) & (not isinstance(min, datetime)) ):
            return SchemaColumnRandomDate(name, min, max)
        else:
            raise TypeError(f"Unsupported types for random column: min={type(min)}, max={type(max)}")


# Selection Schema Column Class
# This column allows the user to supply a list of possible valued to
# the DataGenerator, and one will be randomly selected.

class SchemaColumnSelection(SchemaColumn):
    """An extension of SchemaColumn that represents a column of type Selection in the
    user-provided schema. It takes a list of possible values and randomly picks one for
    each row. All values have the same weight and the same probability of being picked.

    All value in the list must be of the same type.

    Args:
        name (str): Column name
        values (list of Any): List of possible values to be picked.
    """
    def __init__(self, name: str, values: List[Any]):
        if not isinstance(values, List):
            raise TypeError(f"Expected list in column {name}, got {type(values).__name__}")
        
        first_val_type = type(values[0])
        if not all(isinstance(x, first_val_type) for x in values):
            raise TypeError(f"Expected elements of the same type in values for column {name}")

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
            case datetime.datetime:
                return_type = TimestampType()
            case datetime.date:
                return_type = DateType()
            case builtins.bool:
                return_type = BooleanType()
            case _:
                raise TypeError(f"Unsupported value type: {type(first_val)}")
        
        to_udf_selection = F.udf(pick, return_type)
        return to_udf_selection((F.rand() * num_val).cast("int"))
