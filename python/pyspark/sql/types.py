#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import sys
import decimal
import time
import datetime
import calendar
import json
import re
import base64
from array import array
import ctypes
import warnings

if sys.version >= "3":
    long = int
    basestring = unicode = str

from py4j.protocol import register_input_converter
from py4j.java_gateway import JavaClass

from pyspark import SparkContext
from pyspark.serializers import CloudPickleSerializer

__all__ = [
    "DataType", "NullType", "StringType", "BinaryType", "BooleanType", "DateType",
    "TimestampType", "DecimalType", "DoubleType", "FloatType", "ByteType", "IntegerType",
    "LongType", "ShortType", "ArrayType", "MapType", "StructField", "StructType"]


class DataType(object):
    """Base class for data types."""

    def __repr__(self):
        return self.__class__.__name__

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)

    @classmethod
    def typeName(cls):
        return cls.__name__[:-4].lower()

    def simpleString(self):
        return self.typeName()

    def jsonValue(self):
        return self.typeName()

    def json(self):
        return json.dumps(self.jsonValue(),
                          separators=(',', ':'),
                          sort_keys=True)

    def needConversion(self):
        """
        Does this type needs conversion between Python object and internal SQL object.

        This is used to avoid the unnecessary conversion for ArrayType/MapType/StructType.
        """
        return False

    def toInternal(self, obj):
        """
        Converts a Python object into an internal SQL object.
        """
        return obj

    def fromInternal(self, obj):
        """
        Converts an internal SQL object into a native Python object.
        """
        return obj


# This singleton pattern does not work with pickle, you will get
# another object after pickle and unpickle
class DataTypeSingleton(type):
    """Metaclass for DataType"""

    _instances = {}

    def __call__(cls):
        if cls not in cls._instances:
            cls._instances[cls] = super(DataTypeSingleton, cls).__call__()
        return cls._instances[cls]


class NullType(DataType):
    """Null type.

    The data type representing None, used for the types that cannot be inferred.
    """

    __metaclass__ = DataTypeSingleton


class AtomicType(DataType):
    """An internal type used to represent everything that is not
    null, UDTs, arrays, structs, and maps."""


class NumericType(AtomicType):
    """Numeric data types.
    """


class IntegralType(NumericType):
    """Integral data types.
    """

    __metaclass__ = DataTypeSingleton


class FractionalType(NumericType):
    """Fractional data types.
    """


class StringType(AtomicType):
    """String data type.
    """

    __metaclass__ = DataTypeSingleton


class BinaryType(AtomicType):
    """Binary (byte array) data type.
    """

    __metaclass__ = DataTypeSingleton


class BooleanType(AtomicType):
    """Boolean data type.
    """

    __metaclass__ = DataTypeSingleton


class DateType(AtomicType):
    """Date (datetime.date) data type.
    """

    __metaclass__ = DataTypeSingleton

    EPOCH_ORDINAL = datetime.datetime(1970, 1, 1).toordinal()

    def needConversion(self):
        return True

    def toInternal(self, d):
        if d is not None:
            return d.toordinal() - self.EPOCH_ORDINAL

    def fromInternal(self, v):
        if v is not None:
            return datetime.date.fromordinal(v + self.EPOCH_ORDINAL)


class TimestampType(AtomicType):
    """Timestamp (datetime.datetime) data type.
    """

    __metaclass__ = DataTypeSingleton

    def needConversion(self):
        return True

    def toInternal(self, dt):
        if dt is not None:
            seconds = (calendar.timegm(dt.utctimetuple()) if dt.tzinfo
                       else time.mktime(dt.timetuple()))
            return int(seconds) * 1000000 + dt.microsecond

    def fromInternal(self, ts):
        if ts is not None:
            # using int to avoid precision loss in float
            return datetime.datetime.fromtimestamp(ts // 1000000).replace(microsecond=ts % 1000000)


class DecimalType(FractionalType):
    """Decimal (decimal.Decimal) data type.

    The DecimalType must have fixed precision (the maximum total number of digits)
    and scale (the number of digits on the right of dot). For example, (5, 2) can
    support the value from [-999.99 to 999.99].

    The precision can be up to 38, the scale must be less or equal to precision.

    When creating a DecimalType, the default precision and scale is (10, 0). When inferring
    schema from decimal.Decimal objects, it will be DecimalType(38, 18).

    :param precision: the maximum (i.e. total) number of digits (default: 10)
    :param scale: the number of digits on right side of dot. (default: 0)
    """

    def __init__(self, precision=10, scale=0):
        self.precision = precision
        self.scale = scale
        self.hasPrecisionInfo = True  # this is a public API

    def simpleString(self):
        return "decimal(%d,%d)" % (self.precision, self.scale)

    def jsonValue(self):
        return "decimal(%d,%d)" % (self.precision, self.scale)

    def __repr__(self):
        return "DecimalType(%d,%d)" % (self.precision, self.scale)


class DoubleType(FractionalType):
    """Double data type, representing double precision floats.
    """

    __metaclass__ = DataTypeSingleton


class FloatType(FractionalType):
    """Float data type, representing single precision floats.
    """

    __metaclass__ = DataTypeSingleton


class ByteType(IntegralType):
    """Byte data type, i.e. a signed integer in a single byte.
    """
    def simpleString(self):
        return 'tinyint'


class IntegerType(IntegralType):
    """Int data type, i.e. a signed 32-bit integer.
    """
    def simpleString(self):
        return 'int'


class LongType(IntegralType):
    """Long data type, i.e. a signed 64-bit integer.

    If the values are beyond the range of [-9223372036854775808, 9223372036854775807],
    please use :class:`DecimalType`.
    """
    def simpleString(self):
        return 'bigint'


class ShortType(IntegralType):
    """Short data type, i.e. a signed 16-bit integer.
    """
    def simpleString(self):
        return 'smallint'


class ArrayType(DataType):
    """Array data type.

    :param elementType: :class:`DataType` of each element in the array.
    :param containsNull: boolean, whether the array can contain null (None) values.
    """

    def __init__(self, elementType, containsNull=True):
        """
        >>> ArrayType(StringType()) == ArrayType(StringType(), True)
        True
        >>> ArrayType(StringType(), False) == ArrayType(StringType())
        False
        """
        assert isinstance(elementType, DataType),\
            "elementType %s should be an instance of %s" % (elementType, DataType)
        self.elementType = elementType
        self.containsNull = containsNull

    def simpleString(self):
        return 'array<%s>' % self.elementType.simpleString()

    def __repr__(self):
        return "ArrayType(%s,%s)" % (self.elementType,
                                     str(self.containsNull).lower())

    def jsonValue(self):
        return {"type": self.typeName(),
                "elementType": self.elementType.jsonValue(),
                "containsNull": self.containsNull}

    @classmethod
    def fromJson(cls, json):
        return ArrayType(_parse_datatype_json_value(json["elementType"]),
                         json["containsNull"])

    def needConversion(self):
        return self.elementType.needConversion()

    def toInternal(self, obj):
        if not self.needConversion():
            return obj
        return obj and [self.elementType.toInternal(v) for v in obj]

    def fromInternal(self, obj):
        if not self.needConversion():
            return obj
        return obj and [self.elementType.fromInternal(v) for v in obj]


class MapType(DataType):
    """Map data type.

    :param keyType: :class:`DataType` of the keys in the map.
    :param valueType: :class:`DataType` of the values in the map.
    :param valueContainsNull: indicates whether values can contain null (None) values.

    Keys in a map data type are not allowed to be null (None).
    """

    def __init__(self, keyType, valueType, valueContainsNull=True):
        """
        >>> (MapType(StringType(), IntegerType())
        ...        == MapType(StringType(), IntegerType(), True))
        True
        >>> (MapType(StringType(), IntegerType(), False)
        ...        == MapType(StringType(), FloatType()))
        False
        """
        assert isinstance(keyType, DataType),\
            "keyType %s should be an instance of %s" % (keyType, DataType)
        assert isinstance(valueType, DataType),\
            "valueType %s should be an instance of %s" % (valueType, DataType)
        self.keyType = keyType
        self.valueType = valueType
        self.valueContainsNull = valueContainsNull

    def simpleString(self):
        return 'map<%s,%s>' % (self.keyType.simpleString(), self.valueType.simpleString())

    def __repr__(self):
        return "MapType(%s,%s,%s)" % (self.keyType, self.valueType,
                                      str(self.valueContainsNull).lower())

    def jsonValue(self):
        return {"type": self.typeName(),
                "keyType": self.keyType.jsonValue(),
                "valueType": self.valueType.jsonValue(),
                "valueContainsNull": self.valueContainsNull}

    @classmethod
    def fromJson(cls, json):
        return MapType(_parse_datatype_json_value(json["keyType"]),
                       _parse_datatype_json_value(json["valueType"]),
                       json["valueContainsNull"])

    def needConversion(self):
        return self.keyType.needConversion() or self.valueType.needConversion()

    def toInternal(self, obj):
        if not self.needConversion():
            return obj
        return obj and dict((self.keyType.toInternal(k), self.valueType.toInternal(v))
                            for k, v in obj.items())

    def fromInternal(self, obj):
        if not self.needConversion():
            return obj
        return obj and dict((self.keyType.fromInternal(k), self.valueType.fromInternal(v))
                            for k, v in obj.items())


class StructField(DataType):
    """A field in :class:`StructType`.

    :param name: string, name of the field.
    :param dataType: :class:`DataType` of the field.
    :param nullable: boolean, whether the field can be null (None) or not.
    :param metadata: a dict from string to simple type that can be toInternald to JSON automatically
    """

    def __init__(self, name, dataType, nullable=True, metadata=None):
        """
        >>> (StructField("f1", StringType(), True)
        ...      == StructField("f1", StringType(), True))
        True
        >>> (StructField("f1", StringType(), True)
        ...      == StructField("f2", StringType(), True))
        False
        """
        assert isinstance(dataType, DataType),\
            "dataType %s should be an instance of %s" % (dataType, DataType)
        assert isinstance(name, basestring), "field name %s should be string" % (name)
        if not isinstance(name, str):
            name = name.encode('utf-8')
        self.name = name
        self.dataType = dataType
        self.nullable = nullable
        self.metadata = metadata or {}

    def simpleString(self):
        return '%s:%s' % (self.name, self.dataType.simpleString())

    def __repr__(self):
        return "StructField(%s,%s,%s)" % (self.name, self.dataType,
                                          str(self.nullable).lower())

    def jsonValue(self):
        return {"name": self.name,
                "type": self.dataType.jsonValue(),
                "nullable": self.nullable,
                "metadata": self.metadata}

    @classmethod
    def fromJson(cls, json):
        return StructField(json["name"],
                           _parse_datatype_json_value(json["type"]),
                           json["nullable"],
                           json["metadata"])

    def needConversion(self):
        return self.dataType.needConversion()

    def toInternal(self, obj):
        return self.dataType.toInternal(obj)

    def fromInternal(self, obj):
        return self.dataType.fromInternal(obj)

    def typeName(self):
        raise TypeError(
            "StructField does not have typeName. "
            "Use typeName on its type explicitly instead.")


class StructType(DataType):
    """Struct type, consisting of a list of :class:`StructField`.

    This is the data type representing a :class:`Row`.

    Iterating a :class:`StructType` will iterate over its :class:`StructField`\\s.
    A contained :class:`StructField` can be accessed by its name or position.

    >>> struct1 = StructType([StructField("f1", StringType(), True)])
    >>> struct1["f1"]
    StructField(f1,StringType,true)
    >>> struct1[0]
    StructField(f1,StringType,true)
    """
    def __init__(self, fields=None):
        """
        >>> struct1 = StructType([StructField("f1", StringType(), True)])
        >>> struct2 = StructType([StructField("f1", StringType(), True)])
        >>> struct1 == struct2
        True
        >>> struct1 = StructType([StructField("f1", StringType(), True)])
        >>> struct2 = StructType([StructField("f1", StringType(), True),
        ...     StructField("f2", IntegerType(), False)])
        >>> struct1 == struct2
        False
        """
        if not fields:
            self.fields = []
            self.names = []
        else:
            self.fields = fields
            self.names = [f.name for f in fields]
            assert all(isinstance(f, StructField) for f in fields),\
                "fields should be a list of StructField"
        # Precalculated list of fields that need conversion with fromInternal/toInternal functions
        self._needConversion = [f.needConversion() for f in self]
        self._needSerializeAnyField = any(self._needConversion)

        # TODO(wraschkowski): Remove once we drop support for Python 3.5 and lower
        #
        # This is Palantir's. When this flag is set, rows created with named arguments
        # are have their fields re-ordered such that they match the struct names / schema.
        # E.g. if this struct's names are ['b','a'], then Row(a='a',b='b') becomes in ('b','a').
        #
        # For Spark 3.1 we want to deprecate any reordering, but to do so safely we need to check
        # for out-of-order named arguments and ask users to reorder by hand. But hand reordering
        # isn't an option in Python <3.6 because it doesn't preserve kwargs order.
        self._coerce_rows_enabled = \
            os.environ.get("PYSPARK_COERCE_ROWS_TO_SCHEMA", "false").lower() == "true"

        # TODO(wraschkowski): Remove once we remediated potential corruptions after shipping Spark 3
        #
        # This is Palantir's. When this flag is set and rows converted to a schema, as e.g. in
        # `createDataFrame`, we check if a silent corruption could have occurred in previous jobs.
        # See `_check_row_schema_corruption` for more context.
        self._should_check_row_schema_corruption = \
            os.environ.get("PYSPARK_CHECK_ROW_SCHEMA_CORRUPTION", "false").lower() == "true"

    def add(self, field, data_type=None, nullable=True, metadata=None):
        """
        Construct a StructType by adding new elements to it, to define the schema.
        The method accepts either:

            a) A single parameter which is a StructField object.
            b) Between 2 and 4 parameters as (name, data_type, nullable (optional),
               metadata(optional). The data_type parameter may be either a String or a
               DataType object.

        >>> struct1 = StructType().add("f1", StringType(), True).add("f2", StringType(), True, None)
        >>> struct2 = StructType([StructField("f1", StringType(), True), \\
        ...     StructField("f2", StringType(), True, None)])
        >>> struct1 == struct2
        True
        >>> struct1 = StructType().add(StructField("f1", StringType(), True))
        >>> struct2 = StructType([StructField("f1", StringType(), True)])
        >>> struct1 == struct2
        True
        >>> struct1 = StructType().add("f1", "string", True)
        >>> struct2 = StructType([StructField("f1", StringType(), True)])
        >>> struct1 == struct2
        True

        :param field: Either the name of the field or a StructField object
        :param data_type: If present, the DataType of the StructField to create
        :param nullable: Whether the field to add should be nullable (default True)
        :param metadata: Any additional metadata (default None)
        :return: a new updated StructType
        """
        if isinstance(field, StructField):
            self.fields.append(field)
            self.names.append(field.name)
        else:
            if isinstance(field, str) and data_type is None:
                raise ValueError("Must specify DataType if passing name of struct_field to create.")

            if isinstance(data_type, str):
                data_type_f = _parse_datatype_json_value(data_type)
            else:
                data_type_f = data_type
            self.fields.append(StructField(field, data_type_f, nullable, metadata))
            self.names.append(field)
        # Precalculated list of fields that need conversion with fromInternal/toInternal functions
        self._needConversion = [f.needConversion() for f in self]
        self._needSerializeAnyField = any(self._needConversion)
        return self

    def __iter__(self):
        """Iterate the fields"""
        return iter(self.fields)

    def __len__(self):
        """Return the number of fields."""
        return len(self.fields)

    def __getitem__(self, key):
        """Access fields by name or slice."""
        if isinstance(key, str):
            for field in self:
                if field.name == key:
                    return field
            raise KeyError('No StructField named {0}'.format(key))
        elif isinstance(key, int):
            try:
                return self.fields[key]
            except IndexError:
                raise IndexError('StructType index out of range')
        elif isinstance(key, slice):
            return StructType(self.fields[key])
        else:
            raise TypeError('StructType keys should be strings, integers or slices')

    def simpleString(self):
        return 'struct<%s>' % (','.join(f.simpleString() for f in self))

    def __repr__(self):
        return ("StructType(List(%s))" %
                ",".join(str(field) for field in self))

    def jsonValue(self):
        return {"type": self.typeName(),
                "fields": [f.jsonValue() for f in self]}

    @classmethod
    def fromJson(cls, json):
        return StructType([StructField.fromJson(f) for f in json["fields"]])

    def fieldNames(self):
        """
        Returns all field names in a list.

        >>> struct = StructType([StructField("f1", StringType(), True)])
        >>> struct.fieldNames()
        ['f1']
        """
        return list(self.names)

    def needConversion(self):
        # We need convert Row()/namedtuple into tuple()
        return True

    def toInternal(self, obj):
        if obj is None:
            return

        if self._should_check_row_schema_corruption and isinstance(obj, Row):
            self._check_row_schema_corruption(obj)

        if self._needSerializeAnyField:
            # Only calling toInternal function for fields that need conversion
            if isinstance(obj, dict):
                return tuple(f.toInternal(obj.get(n)) if c else obj.get(n)
                             for n, f, c in zip(self.names, self.fields, self._needConversion))
            elif isinstance(obj, Row) and self._coerce_rows_enabled:
                return tuple(f.toInternal(obj[n]) if c else obj[n]
                             for n, f, c in zip(self.names, self.fields, self._needConversion))
            elif isinstance(obj, (tuple, list)):
                return tuple(f.toInternal(v) if c else v
                             for f, v, c in zip(self.fields, obj, self._needConversion))
            elif hasattr(obj, "__dict__"):
                d = obj.__dict__
                return tuple(f.toInternal(d.get(n)) if c else d.get(n)
                             for n, f, c in zip(self.names, self.fields, self._needConversion))
            else:
                raise ValueError("Unexpected tuple %r with StructType" % obj)
        else:
            if isinstance(obj, dict):
                return tuple(obj.get(n) for n in self.names)
            elif isinstance(obj, Row) \
                    and (getattr(obj, "__from_dict__", False) or self._coerce_rows_enabled):
                return tuple(obj[n] for n in self.names)
            elif isinstance(obj, (list, tuple)):
                return tuple(obj)
            elif hasattr(obj, "__dict__"):
                d = obj.__dict__
                return tuple(d.get(n) for n in self.names)
            else:
                raise ValueError("Unexpected tuple %r with StructType" % obj)

    def fromInternal(self, obj):
        if obj is None:
            return
        if isinstance(obj, Row):
            # it's already converted by pickler
            return obj
        if self._needSerializeAnyField:
            # Only calling fromInternal function for fields that need conversion
            values = [f.fromInternal(v) if c else v
                      for f, v, c in zip(self.fields, obj, self._needConversion)]
        else:
            values = obj
        return _create_row(self.names, values)

    def _check_row_schema_corruption(self, row):
        """
        Check and raise if changed row field ordering could have caused corruption in previous jobs.

        When moving our fork to Spark 3 we introduced a silent behavior change in the order of row
        values. When creating dataframes from rows, this could have caused values to go into the
        wrong columns. Previously, when assigning a `Row(b=2,a=1)` to schema `"a INT, b INT"`, we
        made sure that the right values go into the right columns, even if the order of values in
        the row differed from the order of columns in the schema. That was different from upstream.

        After removing the old behavior, we reverted to using upstream Spark 3's behavior of not
        alphabetically sorting named arguments of rows. So `Row(b=2,a=1)` would be read as `(2,1)`.
        And we then, for a period, enabled sorting (via `PYSPARK_ROW_FIELD_SORTING_ENABLED`) which
        caused the above row to be read as `Row(1,2)`. Both of these were changes relative to the
        previous behavior in our fork. In some cases that change could have been silent and this
        check is to detect those cases.

        Various circumstances affect whether corruption could have occurred (more detail inline):

          * Spark 3 applies some automatic order correction in certain conditions, i.e. making sure
            values go into the right columns based on the name. That depends on the schema types,
            the Python version, and whether row field sorting is enabled. When those conditions
            are met we wouldn't have seen corruption.
          * When re-ordering happens, the most likely outcome is that the values in the new order
            don't match the schema types. That would have caused a type error and prevented
            corrupt data from being written.
          * Coincidentally, it's possible that corruption didn't occur because the new order
            happens to match the schema. E.g. when the schema names are alphabetically sorted,
            row field sorting would happen to be correct.

        """
        # TODO(wraschkowski): Remove this check after completing remediation

        # Was row created with named arguments (kwargs)
        if not hasattr(row, "__fields__"):
            # __fields__ is only set in Row.__new__ when created with kwargs
            return
        # Does row contain all schema fields? If not, re-ordering in Spark 2 could not have worked,
        # because for schema `'a INT'` and `Row(b=1)`, we would have failed on `Row(b=1)['a']`.
        if not set(self.names).issubset(row.__fields__):
            return
        # In Python versions with non-deterministic kwargs order, Spark always sorts kwargs when
        # creating rows. That means we don't need to check for corruption without sorting.
        always_sorted_kwargs = sys.version_info[:2] < (3, 6)
        # For sorted kwargs and non-serializable schema types, Spark 3 reorders row values to match
        # the schema (else-branch in StructType.toInternal). Serializable types are dates and
        # timestamps, so e.g. 'a INT, b STRING' on Python 2 will always be reordered correctly.
        if always_sorted_kwargs and not self._needSerializeAnyField:
            return

        # Create type verifier that validates obj types against this struct/schema. The same is used
        # is used by Spark in `createDataFrame`.
        type_verifier = _make_type_verifier(self)

        # The verifier raises errors on mismatch; we catch to get a bool instead.
        def matches_schema(obj):
            try:
                type_verifier(obj)
                return True
            except (TypeError, ValueError):
                return False

        # Check if corruption would have happened when row field sorting was enabled, i.e. if
        # `PYSPARK_ROW_FIELD_SORTING_ENABLED` was set to `TRUE`.
        def is_wrong_with_row_field_sorting():
            # For non-serializable types (ints, strings, etc.), Spark will reorder row values to
            # match the schema.
            if not self._needSerializeAnyField:
                return False
            # If the alphabetical order of kwargs matches the schema fields the order would
            # have been correct.
            sorted_fields = sorted(row.__fields__)
            if sorted_fields == self.names:
                return False
            # If types in sorted order match the schema, we would have written values in the wrong
            # columns. Otherwise the write would fail and no corruption happen.
            return matches_schema(tuple(row[field] for field in sorted_fields))

        # Check if corruption would have happened with row field sorting disabled.
        def is_wrong_without_row_field_sorting():
            if row.__fields__ == self.names:
                # The order of kwargs matches the schema, so the order would have been correct.
                return False
            # Check if we could have written values in the wrong columns.
            return matches_schema(tuple(row))

        corrupt_with_sorting = is_wrong_with_row_field_sorting()
        corrupt_without_sorting = not always_sorted_kwargs and is_wrong_without_row_field_sorting()
        if corrupt_with_sorting or corrupt_without_sorting:
            raise PalantirRowSchemaMismatch(
                row, self,
                when_sorted=corrupt_with_sorting, when_unsorted=corrupt_without_sorting)


class PalantirRowSchemaMismatch(Exception):
    """Error thrown when a potential schema mismatch could have occurred silently."""
    # TODO(wraschkowski): Remove, see `StructType._check_row_schema_corruption` for context

    def __init__(self, row, schema, when_sorted, when_unsorted):
        super(PalantirRowSchemaMismatch, self).__init__(
            "Detected potential mismatch between schema and named arguments in row: {0} and {1}."
            "\nAs work-around, use positional instead of named arguments and ensure that the order "
            "of values matches the schema.\nE.g. for schema ['a','b'] change Row(b=2,a=1) to "
            "Row(1,2).\nIf that's not possible, please file support ticket with Palantir."
            .format(schema.simpleString(), row))
        self.row = row
        self.schema = schema
        self.when_sorted = when_sorted
        """Whether mismatch would occur when the row field sorting is enabled."""
        self.when_unsorted = when_unsorted
        """Whether mismatch would occur when the row field sorting is disabled."""


class UserDefinedType(DataType):
    """User-defined type (UDT).

    .. note:: WARN: Spark Internal Use Only
    """

    @classmethod
    def typeName(cls):
        return cls.__name__.lower()

    @classmethod
    def sqlType(cls):
        """
        Underlying SQL storage type for this UDT.
        """
        raise NotImplementedError("UDT must implement sqlType().")

    @classmethod
    def module(cls):
        """
        The Python module of the UDT.
        """
        raise NotImplementedError("UDT must implement module().")

    @classmethod
    def scalaUDT(cls):
        """
        The class name of the paired Scala UDT (could be '', if there
        is no corresponding one).
        """
        return ''

    def needConversion(self):
        return True

    @classmethod
    def _cachedSqlType(cls):
        """
        Cache the sqlType() into class, because it's heavily used in `toInternal`.
        """
        if not hasattr(cls, "_cached_sql_type"):
            cls._cached_sql_type = cls.sqlType()
        return cls._cached_sql_type

    def toInternal(self, obj):
        if obj is not None:
            return self._cachedSqlType().toInternal(self.serialize(obj))

    def fromInternal(self, obj):
        v = self._cachedSqlType().fromInternal(obj)
        if v is not None:
            return self.deserialize(v)

    def serialize(self, obj):
        """
        Converts a user-type object into a SQL datum.
        """
        raise NotImplementedError("UDT must implement toInternal().")

    def deserialize(self, datum):
        """
        Converts a SQL datum into a user-type object.
        """
        raise NotImplementedError("UDT must implement fromInternal().")

    def simpleString(self):
        return 'udt'

    def json(self):
        return json.dumps(self.jsonValue(), separators=(',', ':'), sort_keys=True)

    def jsonValue(self):
        if self.scalaUDT():
            assert self.module() != '__main__', 'UDT in __main__ cannot work with ScalaUDT'
            schema = {
                "type": "udt",
                "class": self.scalaUDT(),
                "pyClass": "%s.%s" % (self.module(), type(self).__name__),
                "sqlType": self.sqlType().jsonValue()
            }
        else:
            ser = CloudPickleSerializer()
            b = ser.dumps(type(self))
            schema = {
                "type": "udt",
                "pyClass": "%s.%s" % (self.module(), type(self).__name__),
                "serializedClass": base64.b64encode(b).decode('utf8'),
                "sqlType": self.sqlType().jsonValue()
            }
        return schema

    @classmethod
    def fromJson(cls, json):
        pyUDT = str(json["pyClass"])  # convert unicode to str
        split = pyUDT.rfind(".")
        pyModule = pyUDT[:split]
        pyClass = pyUDT[split+1:]
        m = __import__(pyModule, globals(), locals(), [pyClass])
        if not hasattr(m, pyClass):
            s = base64.b64decode(json['serializedClass'].encode('utf-8'))
            UDT = CloudPickleSerializer().loads(s)
        else:
            UDT = getattr(m, pyClass)
        return UDT()

    def __eq__(self, other):
        return type(self) == type(other)


_atomic_types = [StringType, BinaryType, BooleanType, DecimalType, FloatType, DoubleType,
                 ByteType, ShortType, IntegerType, LongType, DateType, TimestampType, NullType]
_all_atomic_types = dict((t.typeName(), t) for t in _atomic_types)
_all_complex_types = dict((v.typeName(), v)
                          for v in [ArrayType, MapType, StructType])


_FIXED_DECIMAL = re.compile(r"decimal\(\s*(\d+)\s*,\s*(-?\d+)\s*\)")


def _parse_datatype_string(s):
    """
    Parses the given data type string to a :class:`DataType`. The data type string format equals
    :class:`DataType.simpleString`, except that the top level struct type can omit
    the ``struct<>`` and atomic types use ``typeName()`` as their format, e.g. use ``byte`` instead
    of ``tinyint`` for :class:`ByteType`. We can also use ``int`` as a short name
    for :class:`IntegerType`. Since Spark 2.3, this also supports a schema in a DDL-formatted
    string and case-insensitive strings.

    >>> _parse_datatype_string("int ")
    IntegerType
    >>> _parse_datatype_string("INT ")
    IntegerType
    >>> _parse_datatype_string("a: byte, b: decimal(  16 , 8   ) ")
    StructType(List(StructField(a,ByteType,true),StructField(b,DecimalType(16,8),true)))
    >>> _parse_datatype_string("a DOUBLE, b STRING")
    StructType(List(StructField(a,DoubleType,true),StructField(b,StringType,true)))
    >>> _parse_datatype_string("a: array< short>")
    StructType(List(StructField(a,ArrayType(ShortType,true),true)))
    >>> _parse_datatype_string(" map<string , string > ")
    MapType(StringType,StringType,true)

    >>> # Error cases
    >>> _parse_datatype_string("blabla") # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ParseException:...
    >>> _parse_datatype_string("a: int,") # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ParseException:...
    >>> _parse_datatype_string("array<int") # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ParseException:...
    >>> _parse_datatype_string("map<int, boolean>>") # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ParseException:...
    """
    sc = SparkContext._active_spark_context

    def from_ddl_schema(type_str):
        return _parse_datatype_json_string(
            sc._jvm.org.apache.spark.sql.types.StructType.fromDDL(type_str).json())

    def from_ddl_datatype(type_str):
        return _parse_datatype_json_string(
            sc._jvm.org.apache.spark.sql.api.python.PythonSQLUtils.parseDataType(type_str).json())

    try:
        # DDL format, "fieldname datatype, fieldname datatype".
        return from_ddl_schema(s)
    except Exception as e:
        try:
            # For backwards compatibility, "integer", "struct<fieldname: datatype>" and etc.
            return from_ddl_datatype(s)
        except:
            try:
                # For backwards compatibility, "fieldname: datatype, fieldname: datatype" case.
                return from_ddl_datatype("struct<%s>" % s.strip())
            except:
                raise e


def _parse_datatype_json_string(json_string):
    """Parses the given data type JSON string.
    >>> import pickle
    >>> def check_datatype(datatype):
    ...     pickled = pickle.loads(pickle.dumps(datatype))
    ...     assert datatype == pickled
    ...     scala_datatype = spark._jsparkSession.parseDataType(datatype.json())
    ...     python_datatype = _parse_datatype_json_string(scala_datatype.json())
    ...     assert datatype == python_datatype
    >>> for cls in _all_atomic_types.values():
    ...     check_datatype(cls())

    >>> # Simple ArrayType.
    >>> simple_arraytype = ArrayType(StringType(), True)
    >>> check_datatype(simple_arraytype)

    >>> # Simple MapType.
    >>> simple_maptype = MapType(StringType(), LongType())
    >>> check_datatype(simple_maptype)

    >>> # Simple StructType.
    >>> simple_structtype = StructType([
    ...     StructField("a", DecimalType(), False),
    ...     StructField("b", BooleanType(), True),
    ...     StructField("c", LongType(), True),
    ...     StructField("d", BinaryType(), False)])
    >>> check_datatype(simple_structtype)

    >>> # Complex StructType.
    >>> complex_structtype = StructType([
    ...     StructField("simpleArray", simple_arraytype, True),
    ...     StructField("simpleMap", simple_maptype, True),
    ...     StructField("simpleStruct", simple_structtype, True),
    ...     StructField("boolean", BooleanType(), False),
    ...     StructField("withMeta", DoubleType(), False, {"name": "age"})])
    >>> check_datatype(complex_structtype)

    >>> # Complex ArrayType.
    >>> complex_arraytype = ArrayType(complex_structtype, True)
    >>> check_datatype(complex_arraytype)

    >>> # Complex MapType.
    >>> complex_maptype = MapType(complex_structtype,
    ...                           complex_arraytype, False)
    >>> check_datatype(complex_maptype)
    """
    return _parse_datatype_json_value(json.loads(json_string))


def _parse_datatype_json_value(json_value):
    if not isinstance(json_value, dict):
        if json_value in _all_atomic_types.keys():
            return _all_atomic_types[json_value]()
        elif json_value == 'decimal':
            return DecimalType()
        elif _FIXED_DECIMAL.match(json_value):
            m = _FIXED_DECIMAL.match(json_value)
            return DecimalType(int(m.group(1)), int(m.group(2)))
        else:
            raise ValueError("Could not parse datatype: %s" % json_value)
    else:
        tpe = json_value["type"]
        if tpe in _all_complex_types:
            return _all_complex_types[tpe].fromJson(json_value)
        elif tpe == 'udt':
            return UserDefinedType.fromJson(json_value)
        else:
            raise ValueError("not supported type: %s" % tpe)


# Mapping Python types to Spark SQL DataType
_type_mappings = {
    type(None): NullType,
    bool: BooleanType,
    int: LongType,
    float: DoubleType,
    str: StringType,
    bytearray: BinaryType,
    decimal.Decimal: DecimalType,
    datetime.date: DateType,
    datetime.datetime: TimestampType,
    datetime.time: TimestampType,
}

if sys.version < "3":
    _type_mappings.update({
        unicode: StringType,
        long: LongType,
    })

if sys.version >= "3":
    _type_mappings.update({
        bytes: BinaryType,
    })

# Mapping Python array types to Spark SQL DataType
# We should be careful here. The size of these types in python depends on C
# implementation. We need to make sure that this conversion does not lose any
# precision. Also, JVM only support signed types, when converting unsigned types,
# keep in mind that it require 1 more bit when stored as signed types.
#
# Reference for C integer size, see:
# ISO/IEC 9899:201x specification, chapter 5.2.4.2.1 Sizes of integer types <limits.h>.
# Reference for python array typecode, see:
# https://docs.python.org/2/library/array.html
# https://docs.python.org/3.6/library/array.html
# Reference for JVM's supported integral types:
# http://docs.oracle.com/javase/specs/jvms/se8/html/jvms-2.html#jvms-2.3.1

_array_signed_int_typecode_ctype_mappings = {
    'b': ctypes.c_byte,
    'h': ctypes.c_short,
    'i': ctypes.c_int,
    'l': ctypes.c_long,
}

_array_unsigned_int_typecode_ctype_mappings = {
    'B': ctypes.c_ubyte,
    'H': ctypes.c_ushort,
    'I': ctypes.c_uint,
    'L': ctypes.c_ulong
}


def _int_size_to_type(size):
    """
    Return the Catalyst datatype from the size of integers.
    """
    if size <= 8:
        return ByteType
    if size <= 16:
        return ShortType
    if size <= 32:
        return IntegerType
    if size <= 64:
        return LongType

# The list of all supported array typecodes, is stored here
_array_type_mappings = {
    # Warning: Actual properties for float and double in C is not specified in C.
    # On almost every system supported by both python and JVM, they are IEEE 754
    # single-precision binary floating-point format and IEEE 754 double-precision
    # binary floating-point format. And we do assume the same thing here for now.
    'f': FloatType,
    'd': DoubleType
}

# compute array typecode mappings for signed integer types
for _typecode in _array_signed_int_typecode_ctype_mappings.keys():
    size = ctypes.sizeof(_array_signed_int_typecode_ctype_mappings[_typecode]) * 8
    dt = _int_size_to_type(size)
    if dt is not None:
        _array_type_mappings[_typecode] = dt

# compute array typecode mappings for unsigned integer types
for _typecode in _array_unsigned_int_typecode_ctype_mappings.keys():
    # JVM does not have unsigned types, so use signed types that is at least 1
    # bit larger to store
    size = ctypes.sizeof(_array_unsigned_int_typecode_ctype_mappings[_typecode]) * 8 + 1
    dt = _int_size_to_type(size)
    if dt is not None:
        _array_type_mappings[_typecode] = dt

# Type code 'u' in Python's array is deprecated since version 3.3, and will be
# removed in version 4.0. See: https://docs.python.org/3/library/array.html
if sys.version_info[0] < 4:
    _array_type_mappings['u'] = StringType

# Type code 'c' are only available at python 2
if sys.version_info[0] < 3:
    _array_type_mappings['c'] = StringType

# SPARK-21465:
# In python2, array of 'L' happened to be mistakenly, just partially supported. To
# avoid breaking user's code, we should keep this partial support. Below is a
# dirty hacking to keep this partial support and pass the unit test.
import platform
if sys.version_info[0] < 3 and platform.python_implementation() != 'PyPy':
    if 'L' not in _array_type_mappings.keys():
        _array_type_mappings['L'] = LongType
        _array_unsigned_int_typecode_ctype_mappings['L'] = ctypes.c_uint


def _infer_type(obj):
    """Infer the DataType from obj
    """
    if obj is None:
        return NullType()

    if hasattr(obj, '__UDT__'):
        return obj.__UDT__

    dataType = _type_mappings.get(type(obj))
    if dataType is DecimalType:
        # the precision and scale of `obj` may be different from row to row.
        return DecimalType(38, 18)
    elif dataType is not None:
        return dataType()

    if isinstance(obj, dict):
        for key, value in obj.items():
            if key is not None and value is not None:
                return MapType(_infer_type(key), _infer_type(value), True)
        return MapType(NullType(), NullType(), True)
    elif isinstance(obj, list):
        for v in obj:
            if v is not None:
                return ArrayType(_infer_type(obj[0]), True)
        return ArrayType(NullType(), True)
    elif isinstance(obj, array):
        if obj.typecode in _array_type_mappings:
            return ArrayType(_array_type_mappings[obj.typecode](), False)
        else:
            raise TypeError("not supported type: array(%s)" % obj.typecode)
    else:
        try:
            return _infer_schema(obj)
        except TypeError:
            raise TypeError("not supported type: %s" % type(obj))


def _infer_schema(row, names=None):
    """Infer the schema from dict/namedtuple/object"""
    if isinstance(row, dict):
        items = sorted(row.items())

    elif isinstance(row, (tuple, list)):
        if hasattr(row, "__fields__"):  # Row
            items = zip(row.__fields__, tuple(row))
        elif hasattr(row, "_fields"):  # namedtuple
            items = zip(row._fields, tuple(row))
        else:
            if names is None:
                names = ['_%d' % i for i in range(1, len(row) + 1)]
            elif len(names) < len(row):
                names.extend('_%d' % i for i in range(len(names) + 1, len(row) + 1))
            items = zip(names, row)

    elif hasattr(row, "__dict__"):  # object
        items = sorted(row.__dict__.items())

    else:
        raise TypeError("Can not infer schema for type: %s" % type(row))

    fields = [StructField(k, _infer_type(v), True) for k, v in items]
    return StructType(fields)


def _has_nulltype(dt):
    """ Return whether there is a NullType in `dt` or not """
    if isinstance(dt, StructType):
        return any(_has_nulltype(f.dataType) for f in dt.fields)
    elif isinstance(dt, ArrayType):
        return _has_nulltype((dt.elementType))
    elif isinstance(dt, MapType):
        return _has_nulltype(dt.keyType) or _has_nulltype(dt.valueType)
    else:
        return isinstance(dt, NullType)


def _merge_type(a, b, name=None):
    if name is None:
        new_msg = lambda msg: msg
        new_name = lambda n: "field %s" % n
    else:
        new_msg = lambda msg: "%s: %s" % (name, msg)
        new_name = lambda n: "field %s in %s" % (n, name)

    if isinstance(a, NullType):
        return b
    elif isinstance(b, NullType):
        return a
    elif type(a) is not type(b):
        # TODO: type cast (such as int -> long)
        raise TypeError(new_msg("Can not merge type %s and %s" % (type(a), type(b))))

    # same type
    if isinstance(a, StructType):
        nfs = dict((f.name, f.dataType) for f in b.fields)
        fields = [StructField(f.name, _merge_type(f.dataType, nfs.get(f.name, NullType()),
                                                  name=new_name(f.name)))
                  for f in a.fields]
        names = set([f.name for f in fields])
        for n in nfs:
            if n not in names:
                fields.append(StructField(n, nfs[n]))
        return StructType(fields)

    elif isinstance(a, ArrayType):
        return ArrayType(_merge_type(a.elementType, b.elementType,
                                     name='element in array %s' % name), True)

    elif isinstance(a, MapType):
        return MapType(_merge_type(a.keyType, b.keyType, name='key of map %s' % name),
                       _merge_type(a.valueType, b.valueType, name='value of map %s' % name),
                       True)
    else:
        return a


def _need_converter(dataType):
    if isinstance(dataType, StructType):
        return True
    elif isinstance(dataType, ArrayType):
        return _need_converter(dataType.elementType)
    elif isinstance(dataType, MapType):
        return _need_converter(dataType.keyType) or _need_converter(dataType.valueType)
    elif isinstance(dataType, NullType):
        return True
    else:
        return False


def _create_converter(dataType):
    """Create a converter to drop the names of fields in obj """
    if not _need_converter(dataType):
        return lambda x: x

    if isinstance(dataType, ArrayType):
        conv = _create_converter(dataType.elementType)
        return lambda row: [conv(v) for v in row]

    elif isinstance(dataType, MapType):
        kconv = _create_converter(dataType.keyType)
        vconv = _create_converter(dataType.valueType)
        return lambda row: dict((kconv(k), vconv(v)) for k, v in row.items())

    elif isinstance(dataType, NullType):
        return lambda x: None

    elif not isinstance(dataType, StructType):
        return lambda x: x

    # dataType must be StructType
    names = [f.name for f in dataType.fields]
    converters = [_create_converter(f.dataType) for f in dataType.fields]
    convert_fields = any(_need_converter(f.dataType) for f in dataType.fields)

    def convert_struct(obj):
        if obj is None:
            return

        if isinstance(obj, (tuple, list)):
            if convert_fields:
                return tuple(conv(v) for v, conv in zip(obj, converters))
            else:
                return tuple(obj)

        if isinstance(obj, dict):
            d = obj
        elif hasattr(obj, "__dict__"):  # object
            d = obj.__dict__
        else:
            raise TypeError("Unexpected obj type: %s" % type(obj))

        if convert_fields:
            return tuple([conv(d.get(name)) for name, conv in zip(names, converters)])
        else:
            return tuple([d.get(name) for name in names])

    return convert_struct


_acceptable_types = {
    BooleanType: (bool,),
    ByteType: (int, long),
    ShortType: (int, long),
    IntegerType: (int, long),
    LongType: (int, long),
    FloatType: (float,),
    DoubleType: (float,),
    DecimalType: (decimal.Decimal,),
    StringType: (str, unicode),
    BinaryType: (bytearray, bytes),
    DateType: (datetime.date, datetime.datetime),
    TimestampType: (datetime.datetime,),
    ArrayType: (list, tuple, array),
    MapType: (dict,),
    StructType: (tuple, list, dict),
}


def _make_type_verifier(dataType, nullable=True, name=None):
    """
    Make a verifier that checks the type of obj against dataType and raises a TypeError if they do
    not match.

    This verifier also checks the value of obj against datatype and raises a ValueError if it's not
    within the allowed range, e.g. using 128 as ByteType will overflow. Note that, Python float is
    not checked, so it will become infinity when cast to Java float, if it overflows.

    >>> _make_type_verifier(StructType([]))(None)
    >>> _make_type_verifier(StringType())("")
    >>> _make_type_verifier(LongType())(0)
    >>> _make_type_verifier(LongType())(1 << 64) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> _make_type_verifier(ArrayType(ShortType()))(list(range(3)))
    >>> _make_type_verifier(ArrayType(StringType()))(set()) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    TypeError:...
    >>> _make_type_verifier(MapType(StringType(), IntegerType()))({})
    >>> _make_type_verifier(StructType([]))(())
    >>> _make_type_verifier(StructType([]))([])
    >>> _make_type_verifier(StructType([]))([1]) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> # Check if numeric values are within the allowed range.
    >>> _make_type_verifier(ByteType())(12)
    >>> _make_type_verifier(ByteType())(1234) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> _make_type_verifier(ByteType(), False)(None) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> _make_type_verifier(
    ...     ArrayType(ShortType(), False))([1, None]) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> _make_type_verifier(MapType(StringType(), IntegerType()))({None: 1})
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> schema = StructType().add("a", IntegerType()).add("b", StringType(), False)
    >>> _make_type_verifier(schema)((1, None)) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    """

    if name is None:
        new_msg = lambda msg: msg
        new_name = lambda n: "field %s" % n
    else:
        new_msg = lambda msg: "%s: %s" % (name, msg)
        new_name = lambda n: "field %s in %s" % (n, name)

    def verify_nullability(obj):
        if obj is None:
            if nullable:
                return True
            else:
                raise ValueError(new_msg("This field is not nullable, but got None"))
        else:
            return False

    _type = type(dataType)

    def assert_acceptable_types(obj):
        assert _type in _acceptable_types, \
            new_msg("unknown datatype: %s for object %r" % (dataType, obj))

    def verify_acceptable_types(obj):
        # subclass of them can not be fromInternal in JVM
        if type(obj) not in _acceptable_types[_type]:
            raise TypeError(new_msg("%s can not accept object %r in type %s"
                                    % (dataType, obj, type(obj))))

    if isinstance(dataType, StringType):
        # StringType can work with any types
        verify_value = lambda _: _

    elif isinstance(dataType, UserDefinedType):
        verifier = _make_type_verifier(dataType.sqlType(), name=name)

        def verify_udf(obj):
            if not (hasattr(obj, '__UDT__') and obj.__UDT__ == dataType):
                raise ValueError(new_msg("%r is not an instance of type %r" % (obj, dataType)))
            verifier(dataType.toInternal(obj))

        verify_value = verify_udf

    elif isinstance(dataType, ByteType):
        def verify_byte(obj):
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            if obj < -128 or obj > 127:
                raise ValueError(new_msg("object of ByteType out of range, got: %s" % obj))

        verify_value = verify_byte

    elif isinstance(dataType, ShortType):
        def verify_short(obj):
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            if obj < -32768 or obj > 32767:
                raise ValueError(new_msg("object of ShortType out of range, got: %s" % obj))

        verify_value = verify_short

    elif isinstance(dataType, IntegerType):
        def verify_integer(obj):
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            if obj < -2147483648 or obj > 2147483647:
                raise ValueError(
                    new_msg("object of IntegerType out of range, got: %s" % obj))

        verify_value = verify_integer

    elif isinstance(dataType, LongType):
        def verify_long(obj):
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            if obj < -9223372036854775808 or obj > 9223372036854775807:
                raise ValueError(
                    new_msg("object of LongType out of range, got: %s" % obj))

        verify_value = verify_long

    elif isinstance(dataType, ArrayType):
        element_verifier = _make_type_verifier(
            dataType.elementType, dataType.containsNull, name="element in array %s" % name)

        def verify_array(obj):
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            for i in obj:
                element_verifier(i)

        verify_value = verify_array

    elif isinstance(dataType, MapType):
        key_verifier = _make_type_verifier(dataType.keyType, False, name="key of map %s" % name)
        value_verifier = _make_type_verifier(
            dataType.valueType, dataType.valueContainsNull, name="value of map %s" % name)

        def verify_map(obj):
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            for k, v in obj.items():
                key_verifier(k)
                value_verifier(v)

        verify_value = verify_map

    elif isinstance(dataType, StructType):
        verifiers = []
        for f in dataType.fields:
            verifier = _make_type_verifier(f.dataType, f.nullable, name=new_name(f.name))
            verifiers.append((f.name, verifier))

        def verify_struct(obj):
            assert_acceptable_types(obj)

            if isinstance(obj, dict):
                for f, verifier in verifiers:
                    verifier(obj.get(f))
            elif isinstance(obj, Row) and getattr(obj, "__from_dict__", False):
                # the order in obj could be different than dataType.fields
                for f, verifier in verifiers:
                    verifier(obj[f])
            elif isinstance(obj, (tuple, list)):
                if len(obj) != len(verifiers):
                    raise ValueError(
                        new_msg("Length of object (%d) does not match with "
                                "length of fields (%d)" % (len(obj), len(verifiers))))
                for v, (_, verifier) in zip(obj, verifiers):
                    verifier(v)
            elif hasattr(obj, "__dict__"):
                d = obj.__dict__
                for f, verifier in verifiers:
                    verifier(d.get(f))
            else:
                raise TypeError(new_msg("StructType can not accept object %r in type %s"
                                        % (obj, type(obj))))
        verify_value = verify_struct

    else:
        def verify_default(obj):
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)

        verify_value = verify_default

    def verify(obj):
        if not verify_nullability(obj):
            verify_value(obj)

    return verify


# This is used to unpickle a Row from JVM
def _create_row_inbound_converter(dataType):
    return lambda *a: dataType.fromInternal(a)


def _create_row(fields, values):
    row = Row(*values)
    row.__fields__ = fields
    return row


class Row(tuple):

    """
    A row in :class:`DataFrame`.
    The fields in it can be accessed:

    * like attributes (``row.key``)
    * like dictionary values (``row[key]``)

    ``key in row`` will search through row keys.

    Row can be used to create a row object by using named arguments.
    It is not allowed to omit a named argument to represent that the value is
    None or missing. This should be explicitly set to None in this case.

    NOTE: As of Spark 3.0.0, Rows created from named arguments no longer have
    field names sorted alphabetically and will be ordered in the position as
    entered. To enable sorting for Rows compatible with Spark 2.x, set the
    environment variable "PYSPARK_ROW_FIELD_SORTING_ENABLED" to "true". This
    option is deprecated and will be removed in future versions of Spark. For
    Python versions < 3.6, the order of named arguments is not guaranteed to
    be the same as entered, see https://www.python.org/dev/peps/pep-0468. In
    this case, a warning will be issued and the Row will fallback to sort the
    field names automatically.

    NOTE: Examples with Row in pydocs are run with the environment variable
    "PYSPARK_ROW_FIELD_SORTING_ENABLED" set to "true" which results in output
    where fields are sorted.

    >>> row = Row(name="Alice", age=11)
    >>> row
    Row(age=11, name='Alice')
    >>> row['name'], row['age']
    ('Alice', 11)
    >>> row.name, row.age
    ('Alice', 11)
    >>> 'name' in row
    True
    >>> 'wrong_key' in row
    False

    Row also can be used to create another Row like class, then it
    could be used to create Row objects, such as

    >>> Person = Row("name", "age")
    >>> Person
    <Row('name', 'age')>
    >>> 'name' in Person
    True
    >>> 'wrong_key' in Person
    False
    >>> Person("Alice", 11)
    Row(name='Alice', age=11)

    This form can also be used to create rows as tuple values, i.e. with unnamed
    fields. Beware that such Row objects have different equality semantics:

    >>> row1 = Row("Alice", 11)
    >>> row2 = Row(name="Alice", age=11)
    >>> row1 == row2
    False
    >>> row3 = Row(a="Alice", b=11)
    >>> row1 == row3
    True
    """

    # Remove after Python < 3.6 dropped, see SPARK-29748
    _row_field_sorting_enabled = \
        os.environ.get('PYSPARK_ROW_FIELD_SORTING_ENABLED', 'false').lower() == 'true'

    if _row_field_sorting_enabled:
        warnings.warn("The environment variable 'PYSPARK_ROW_FIELD_SORTING_ENABLED' "
                      "is deprecated and will be removed in future versions of Spark")

    def __new__(cls, *args, **kwargs):
        if args and kwargs:
            raise ValueError("Can not use both args "
                             "and kwargs to create Row")
        if kwargs:
            if not Row._row_field_sorting_enabled and sys.version_info[:2] < (3, 6):
                warnings.warn("To use named arguments for Python version < 3.6, Row fields will be "
                              "automatically sorted. This warning can be skipped by setting the "
                              "environment variable 'PYSPARK_ROW_FIELD_SORTING_ENABLED' to 'true'.")
                Row._row_field_sorting_enabled = True

            # create row objects
            if Row._row_field_sorting_enabled:
                # Remove after Python < 3.6 dropped, see SPARK-29748
                names = sorted(kwargs.keys())
                row = tuple.__new__(cls, [kwargs[n] for n in names])
                row.__fields__ = names
                row.__from_dict__ = True
            else:
                row = tuple.__new__(cls, list(kwargs.values()))
                row.__fields__ = list(kwargs.keys())

            return row
        else:
            # create row class or objects
            return tuple.__new__(cls, args)

    def asDict(self, recursive=False):
        """
        Return as a dict

        :param recursive: turns the nested Rows to dict (default: False).

        .. note:: If a row contains duplicate field names, e.g., the rows of a join
            between two :class:`DataFrame` that both have the fields of same names,
            one of the duplicate fields will be selected by ``asDict``. ``__getitem__``
            will also return one of the duplicate fields, however returned value might
            be different to ``asDict``.

        >>> Row(name="Alice", age=11).asDict() == {'name': 'Alice', 'age': 11}
        True
        >>> row = Row(key=1, value=Row(name='a', age=2))
        >>> row.asDict() == {'key': 1, 'value': Row(age=2, name='a')}
        True
        >>> row.asDict(True) == {'key': 1, 'value': {'name': 'a', 'age': 2}}
        True
        """
        if not hasattr(self, "__fields__"):
            raise TypeError("Cannot convert a Row class into dict")

        if recursive:
            def conv(obj):
                if isinstance(obj, Row):
                    return obj.asDict(True)
                elif isinstance(obj, list):
                    return [conv(o) for o in obj]
                elif isinstance(obj, dict):
                    return dict((k, conv(v)) for k, v in obj.items())
                else:
                    return obj
            return dict(zip(self.__fields__, (conv(o) for o in self)))
        else:
            return dict(zip(self.__fields__, self))

    def __contains__(self, item):
        if hasattr(self, "__fields__"):
            return item in self.__fields__
        else:
            return super(Row, self).__contains__(item)

    # let object acts like class
    def __call__(self, *args):
        """create new Row object"""
        if len(args) > len(self):
            raise ValueError("Can not create Row with fields %s, expected %d values "
                             "but got %s" % (self, len(self), args))
        return _create_row(self, args)

    def __getitem__(self, item):
        if isinstance(item, (int, slice)):
            return super(Row, self).__getitem__(item)
        try:
            # it will be slow when it has many fields,
            # but this will not be used in normal cases
            idx = self.__fields__.index(item)
            return super(Row, self).__getitem__(idx)
        except IndexError:
            raise KeyError(item)
        except ValueError:
            raise ValueError(item)

    def __getattr__(self, item):
        if item.startswith("__"):
            raise AttributeError(item)
        try:
            # it will be slow when it has many fields,
            # but this will not be used in normal cases
            idx = self.__fields__.index(item)
            return self[idx]
        except IndexError:
            raise AttributeError(item)
        except ValueError:
            raise AttributeError(item)

    def __setattr__(self, key, value):
        if key != '__fields__' and key != "__from_dict__":
            raise Exception("Row is read-only")
        self.__dict__[key] = value

    def __reduce__(self):
        """Returns a tuple so Python knows how to pickle Row."""
        if hasattr(self, "__fields__"):
            return (_create_row, (self.__fields__, tuple(self)))
        else:
            return tuple.__reduce__(self)

    def __repr__(self):
        """Printable representation of Row used in Python REPL."""
        if hasattr(self, "__fields__"):
            return "Row(%s)" % ", ".join("%s=%r" % (k, v)
                                         for k, v in zip(self.__fields__, tuple(self)))
        else:
            return "<Row(%s)>" % ", ".join("%r" % field for field in self)


class DateConverter(object):
    def can_convert(self, obj):
        return isinstance(obj, datetime.date)

    def convert(self, obj, gateway_client):
        Date = JavaClass("java.sql.Date", gateway_client)
        return Date.valueOf(obj.strftime("%Y-%m-%d"))


class DatetimeConverter(object):
    def can_convert(self, obj):
        return isinstance(obj, datetime.datetime)

    def convert(self, obj, gateway_client):
        Timestamp = JavaClass("java.sql.Timestamp", gateway_client)
        seconds = (calendar.timegm(obj.utctimetuple()) if obj.tzinfo
                   else time.mktime(obj.timetuple()))
        t = Timestamp(int(seconds) * 1000)
        t.setNanos(obj.microsecond * 1000)
        return t

# datetime is a subclass of date, we should register DatetimeConverter first
register_input_converter(DatetimeConverter())
register_input_converter(DateConverter())


def _test():
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import SparkSession
    globs = globals()
    sc = SparkContext('local[4]', 'PythonTest')
    globs['sc'] = sc
    globs['spark'] = SparkSession.builder.getOrCreate()
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
