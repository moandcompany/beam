
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

"""PTransforms for converting elements into CSV-formatted strings"""

from csv import Dialect
from csv import DictWriter
from csv import excel
from cStringIO import StringIO

from apache_beam import DoFn
from apache_beam import ParDo
from apache_beam import PTransform


# TODO Consider revising implementation for unicode support
def _dict_to_csv(element, column_order, missing_val='', discard_extras=True, dialect=excel):
    """
    Converts a dictionary to a CSV-formatted string

    Note: This implementation does not support Unicode encoding

    :param element: A dictionary containing values for creating a CSV-formatted string
    :param column_order: A list or tuple containing dictionary keys corresponding to CSV-formatted columns, in order
    :param missing_val: Character to be used for indicating missing values
    :param discard_extras: Ignore additional values from input dictionary if found
    :param dialect: Specify customized delimiter, escape chars, etc via a subclass of csv.Dialect
    :return: CSV-formatted string
    """

    if not isinstance(element, dict):
        raise TypeError('element input should be of type dict')
    if not isinstance(column_order, (tuple, list)):
        raise TypeError('column_order must be a tuple or list')
    if len(column_order) < 1:
        raise ValueError('column_order must contain at least one name')
    if not issubclass(dialect, Dialect):
        raise TypeError('dialect should be a subclass of csv.Dialect')

    buf = StringIO()

    writer = DictWriter(buf,
                        fieldnames=column_order,
                        restval=missing_val,
                        extrasaction=('ignore' if discard_extras else 'raise'),
                        dialect=dialect)
    writer.writerow(element)

    return buf.getvalue().rstrip(dialect.lineterminator)


class _DictToCSVFn(DoFn):
    """ A ``DoFn`` that converts a Dictionary to a CSV-formatted String """

    def __init__(self, column_order, missing_val='', discard_extras=True, dialect=excel):
        """
        Initialize the _DictToCSVFn DoFn

        :param column_order: A tuple or list specifying the keys for dictionary values to be formatted as csv, in order
        :param missing_val: Character to be used for indicating missing values
        :param discard_extras: (bool) Behavior when additional values are found in the dictionary input element
        :param dialect: Delimiters, escape-characters, etc can be controlled by providing a subclass of csv.Dialect
        """
        self._column_order = column_order
        self._missing_val = missing_val
        self._discard_extras = discard_extras
        self._dialect = dialect

    def process(self, element, *args, **kwargs):
        result = _dict_to_csv(element,
                              column_order=self._column_order,
                              missing_val=self._missing_val,
                              discard_extras=self._discard_extras,
                              dialect=self._dialect)

        return [result,]


class DictToCSV(PTransform):
    """ A ``PTransform`` that transforms a PCollection of Dictionaries to a PCollection of CSV-formatted Strings """

    def __init__(self, column_order, missing_val='', discard_extras=True, dialect=excel):
        """
        Initializes an instance of the DictToCSV PTransform

        :param column_order: A tuple or list specifying the keys for dictionary values to be formatted as csv, in order
        :param missing_val: Character to be used for indicating missing values
        :param discard_extras: (bool) Behavior when additional values are found in the dictionary input element
        :param dialect: Delimiters, escape-characters, etc can be controlled by providing a subclass of csv.Dialect

        """
        self._column_order = column_order
        self._missing_val = missing_val
        self._discard_extras = discard_extras
        self._dialect = dialect

    def expand(self, pcoll):
        return pcoll | 'ConvertDictsToCSVs' >> ParDo(_DictToCSVFn(column_order=self._column_order,
                                                                  missing_val=self._missing_val,
                                                                  discard_extras=self._discard_extras,
                                                                  dialect=self._dialect)
                                                     )
