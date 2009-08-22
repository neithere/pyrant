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


from props import Property


DISALLOWED_ATTRS = ('__dict__', '__metaclass__', '__module__')
DEFAULT_OPTIONS = ('must_have',)


class ModelOptions(object):
    "Model metadata" # not to be confused with metaclass ;)

    def __init__(self, custom_options_cls):
        self.model_instance = None
        self.prop_names = []
        self.props = {}
        self.must_have = None    # conditions for searching model instances within a storage

        if custom_options_cls:    # the 'class Meta: ...' within model declaration
            custom_options = custom_options_cls.__dict__.copy()
            for name in custom_options_cls.__dict__:
                if name.startswith('_'):
                    del custom_options[name]
            for name in DEFAULT_OPTIONS:
                if name in custom_options:
                    setattr(self, name, custom_options.pop(name))
            if custom_options:
                raise TypeError, "'class Meta' got invalid attribute(s): %s" % ','.join(custom_options.keys())

    def set_model_instance(self, instance):
        self.model_instance = instance
        for prop in self.props.values():
            prop.model_instance = instance

    def add_prop(self, attr_name, prop):
        self.props[attr_name] = prop

        # inform prop about its model
        prop.attr_name = attr_name

        # preserve order in which attributes were declared
        self.prop_names = sorted(self.prop_names + [attr_name],
                                  key=lambda n: self.props[n].creation_cnt)

    def validate_property(self, name):
        # (temporary method? makes sense when saving a model instance)
        assert self.model_instance
        p = self.props[name]
        value = getattr(self.model_instance, name)
        return p.validate(value) # will raise ValidationError if smth is wrong


class ModelBase(type):
    "Metaclass for all models"

    def __new__(cls, name, bases, attrs):
        module = attrs.pop('__module__')

        new_class = type.__new__(cls, name, bases, attrs)

        parents = [b for b in bases if isinstance(b, ModelBase)]
        if not parents:
            return new_class

        # add empty model options (to be populated below)
        attr_meta = attrs.pop('Meta', None)
        setattr(new_class, '_meta', ModelOptions(attr_meta))

        # inherit model options from base classes
        for base in bases:
            if hasattr(base, '_meta'):
                for name in base._meta.prop_names:
                    new_class._meta.add_prop(name, base._meta.props[name])

        # move prop declarations to model options
        for attr, value in attrs.iteritems():
            if attr not in DISALLOWED_ATTRS:
                if isinstance(value, Property):
                    new_class._meta.add_prop(attr, value)
                    setattr(new_class, attr, None)

        # fill some attrs from default search query    XXX  may be undesirable
        if new_class._meta.must_have:
            for k, v in new_class._meta.must_have.items():
                setattr(new_class, k, v)

        return new_class


class Model(object):
    "Wrapper for a record with predefined metadata."

    __metaclass__ = ModelBase

    # Python magic methods

    def __init__(self, key=None, storage=None, **kw):
        if self.__class__ == Model:
            raise NotImplementedError('Model must be subclassed')
        self._meta.set_model_instance(self)
        self._storage = storage
        self._key = key

        ## FIXME Tyrant-/shelve-specific!
        if self._storage and self._key and not kw:
            kw = storage[self._key]
        ##

        self._data = kw.copy() # store the original data intact, even if some of it is not be used

        for name in self._meta.prop_names:
            if name in kw:
                raw_value = kw.pop(name)
                value = self._meta.props[name].to_python(raw_value)
                setattr(self, name, value)
        #if kw:
        #    raise TypeError('"%s" is invalid keyword argument for model init.' % kw.keys()[0])

    def __repr__(self):
        return u'<%s %s>' % (self.__class__.__name__, unicode(self))

    def __unicode__(self):
        return "instance" #str(hash(self))

    # Public methods

    @classmethod
    def within(cls, storage):    # less concise but more appropriate name: get_query_for()
        "Returns a Query instance for all model instances within given storage."
        assert isinstance(cls, ModelBase), 'this method must be called with class, not instance'

        query = storage.query

        def _decorate_item(pk, data):
            return cls(key=pk, storage=storage,
                       **dict((str(k), v) for k, v in data.iteritems()))
                                           #if k in cls._meta.prop_names))
        # FIXME make this more flexible or just rely on backend wrapper:
        query._decorate = _decorate_item

        if cls._meta.must_have:
            return query.filter(**cls._meta.must_have)
        return query

    def save(self, storage, sync=True):
        data = self._data.copy()

        for name in self._meta.prop_names:
            value = self._meta.validate_property(name)
            ## FIXME Tyrant-specific: None-->'None' is bad, force None-->''
            if value is None:
                value = ''
            ##
            data[name] = value

        if self._meta.must_have:
            for name in self._meta.must_have.keys():
                if name not in data:
                    data[name] = getattr(self, name)    # NOTE validation should be done using must_have constraints

        ### FIXME Tyrant- or shelve-specific! a backend wrapper should be introduced.
        #         TODO check if this is a correct way of generating an autoincremented pk
        #         ...isn't! Table database supports "setindex", "search", "genuid".
        if not self._key:
            model_label = self.__class__.__name__.lower()
            max_key = len(storage.prefix_keys(model_label))
            self._key = '%s_%s' % (model_label, max_key)
        storage[self._key] = data
        if sync:
            storage.sync()
        ###
