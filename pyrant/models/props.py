# -*- coding: utf-8 -*-
#
#  Copyright (c) 2009 Andy Mikhailenko <neithere@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

class ValidationError(Exception):
    pass


class Property(object):
    "A model property"

    creation_cnt = 0

    def __init__(self, required=False):
        self.required = required

        # info about model we are assigned to -- to be filled from outside
        self.model_instance = None
        self.attr_name = None

        # count field instances to preserve order in which they were declared
        self.creation_cnt = Property.creation_cnt
        Property.creation_cnt += 1

    def to_python(self, value):
        "Converts incoming data into correct Python form."
        raise NotImplementedError

    def validate(self, value):
        assert self.model_instance and self.attr_name, 'model must be initialized'

        # validate empty
        if value is None:
            if self.required:
                raise ValidationError('field is required')
            return
        # validate non-empty (will raise ValidationError on bad value)
        #self.to_python(value)
        if self.check(value):
            raise ValueError('validate() must return None or raise ValidationError')
        return value

    def check(self, value):
        pass
