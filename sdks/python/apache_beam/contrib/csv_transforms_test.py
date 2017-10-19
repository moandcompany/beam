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

import unittest

from csv import excel_tab
from csv_transforms import _dict_to_csv


class DictToCSVTest(unittest.TestCase):

    def test_non_dict_input(self):

        TEST_ELEMENT = None
        TEST_COLUMNS = ('herp', 'derp')

        kwargs = {'element': TEST_ELEMENT, 'column_order': TEST_COLUMNS, 'discard_extras': True}

        self.assertRaises(TypeError, _dict_to_csv, **kwargs)


    def test_empty_dict_input(self):

        TEST_ELEMENT = dict()
        TEST_COLUMNS = ('herp', 'derp')

        kwargs = {'element': TEST_ELEMENT, 'column_order': TEST_COLUMNS, 'discard_extras': True}

        res = _dict_to_csv(**kwargs)

        self.assertEquals(res, ',')


    def test_unmatched_columns_input(self):

        TEST_ELEMENT = dict([('blerp', 3)])
        TEST_COLUMNS = ('herp', 'derp')

        kwargs = {'element': TEST_ELEMENT, 'column_order': TEST_COLUMNS, 'discard_extras': True}

        res = _dict_to_csv(**kwargs)

        self.assertEquals(res, ',')


    def test_empty_columns_input(self):

        TEST_ELEMENT = dict()
        TEST_COLUMNS = tuple()

        kwargs = {'element': TEST_ELEMENT, 'column_order': TEST_COLUMNS, 'discard_extras': True}

        self.assertRaises(ValueError, _dict_to_csv, **kwargs)


    def test_basic_input(self):

        TEST_ELEMENT = dict([('herp', 1), ('derp', 2)])
        TEST_COLUMNS = ('herp', 'derp')

        kwargs = {'element': TEST_ELEMENT, 'column_order': TEST_COLUMNS, 'discard_extras': True}

        res = _dict_to_csv(**kwargs)

        self.assertEquals(res, '1,2')


    def test_basic_input_reversed(self):

        TEST_ELEMENT = dict([('herp', 1), ('derp', 2)])
        TEST_COLUMNS = ('derp', 'herp')

        kwargs = {'element': TEST_ELEMENT, 'column_order': TEST_COLUMNS, 'discard_extras': True}

        res = _dict_to_csv(**kwargs)

        self.assertEquals(res, '2,1')


    def test_basic_input_repeat_columns(self):

        TEST_ELEMENT = dict([('herp', 1), ('derp', 2)])
        TEST_COLUMNS = ('herp', 'herp')

        kwargs = {'element': TEST_ELEMENT, 'column_order': TEST_COLUMNS, 'discard_extras': True}

        res = _dict_to_csv(**kwargs)

        self.assertEquals(res, '1,1')


    def test_basic_input_discard_extras(self):

        TEST_ELEMENT = dict([('herp', 1), ('derp', 2), ('blerp', 3)])
        TEST_COLUMNS = ('herp', 'derp')

        kwargs = {'element': TEST_ELEMENT, 'column_order': TEST_COLUMNS, 'discard_extras': True}

        res = _dict_to_csv(**kwargs)

        self.assertEquals(res, '1,2')


    def test_basic_input_raise_if_extras(self):

        TEST_ELEMENT = dict([('herp', 1), ('derp', 2), ('blerp', 3)])
        TEST_COLUMNS = ('herp', 'derp')

        kwargs = {'element': TEST_ELEMENT, 'column_order': TEST_COLUMNS, 'discard_extras': False}

        self.assertRaises(ValueError, _dict_to_csv, **kwargs)


    def test_basic_input_tab_delimiter(self):

        TEST_ELEMENT = dict([('herp', 1), ('derp', 2)])
        TEST_COLUMNS = ('herp', 'derp')

        kwargs = {'element': TEST_ELEMENT, 'column_order': TEST_COLUMNS, 'dialect': excel_tab}

        res = _dict_to_csv(**kwargs)

        self.assertEquals(res, '1\t2')


    def test_escaped_input(self):

        TEST_ELEMENT = dict([('herp', '1,'), ('derp', 2)])
        TEST_COLUMNS = ('herp', 'derp')

        kwargs = {'element': TEST_ELEMENT, 'column_order': TEST_COLUMNS}

        res = _dict_to_csv(**kwargs)

        self.assertEquals(res, '"1,",2')


    def test_escaped_input_many(self):

        TEST_ELEMENT = dict([('herp', '1,,,'), ('derp', 2)])
        TEST_COLUMNS = ('herp', 'derp')

        kwargs = {'element': TEST_ELEMENT, 'column_order': TEST_COLUMNS}

        res = _dict_to_csv(**kwargs)

        self.assertEquals(res, '"1,,,",2')


    def test_mixed_input(self):

        TEST_ELEMENT = dict([('herp', 1), ('derp', 'somestring'), ('blerp', -1.0)])
        TEST_COLUMNS = ('herp', 'derp', 'blerp')

        kwargs = {'element': TEST_ELEMENT, 'column_order': TEST_COLUMNS, 'discard_extras': True}

        res = _dict_to_csv(**kwargs)

        self.assertEquals(res, '1,somestring,-1.0')


    def test_mixed_input_2(self):

        TEST_ELEMENT = dict([('herp', None), ('derp', True), ('blerp', [1,2,3]), ('merp', (1,2,3))])
        TEST_COLUMNS = ('herp', 'derp', 'blerp', 'merp')

        kwargs = {'element': TEST_ELEMENT, 'column_order': TEST_COLUMNS, 'discard_extras': True}

        res = _dict_to_csv(**kwargs)

        self.assertEquals(res, ',True,"[1, 2, 3]","(1, 2, 3)"')


    def test_nested_dict_input(self):

        TEST_ELEMENT = dict([('herp', 1), ('derp', {'nested': 'dict'})])
        TEST_COLUMNS = ('herp', 'derp')

        kwargs = {'element': TEST_ELEMENT, 'column_order': TEST_COLUMNS, 'discard_extras': True}

        res = _dict_to_csv(**kwargs)

        self.assertEquals(res, '1,{\'nested\': \'dict\'}')


    def test_non_string_keyed_input(self):

        TEST_ELEMENT = dict([(1, 'herp'), (2, 'derp')])
        TEST_COLUMNS = (1, 2)

        kwargs = {'element': TEST_ELEMENT, 'column_order': TEST_COLUMNS, 'discard_extras': True}

        res = _dict_to_csv(**kwargs)

        self.assertEquals(res, 'herp,derp')


if __name__ == '__main__':
    unittest.main()
