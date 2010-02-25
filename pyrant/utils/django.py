# -*- coding: utf-8 -*-

__all__ = ['prepare_django_object', 'import_django_models']


DEFAULT_RENAMINGS = {
    '_state': None,
    'id': None,
}


def get_field_meta(instance, name):
    if name in instance._meta.get_all_field_names():
        return instance._meta.get_field_by_name(name)[0]
    return None

def make_obj_pk(instance):
    return make_pk(instance._meta.object_name, instance.pk)

def make_pk(obj_name, obj_pk):
    return u'%s:%s' % (obj_name, obj_pk)

def prepare_django_object(instance, renamings=None):
    """
    Converts given Django model to a dictionary ready for Tokyo Cabinet.
    Does not save anything, only returns the dictionary.
    """
    orig_data = instance.__dict__
    data = {
        'django_model': instance._meta.object_name,
        'django_id': instance.pk,
    }
    renamings = dict(DEFAULT_RENAMINGS, **renamings or {})
    for key, value in orig_data.iteritems():
        if key.endswith('_id'):
            key = key[:-3]

        field_meta = get_field_meta(instance, key)

        # key conversion
        if key in renamings:
            key = renamings[key]
            if not key:
                continue

        # value conversion
        if field_meta and field_meta.rel:
            if value:
                data[key] = make_pk(field_meta.rel.to._meta.object_name, value)
        else:
            data[key] = '' if value is None else unicode(value)
    return data

def import_django_models(database, models, renamings=None, dry_run=False):
    """
    Converts and saves multiple Django models to Tokyo Cabinet.

    :param tyrant: an instance of pyrant.Tyrant (actually any dict-like object
        will do)
    :param models: a list of Django models to convert. The order does not matter
        because we do not follow foreign keys, we just save their values in the
        same format as document keys, so if the referenced item is ever going
        to be imported, its key will match the value of the reference field.
        I think I should get a nap.
    :param renamings: a dict of dicts, e.g. ``{Note: {'text': 'summary'}}`` which
        reads as "when importing Note objects, rename field `text` to `summary`".
        Setting new value to None (e.g. ``{Note: {'text': None}`}` removes this
        field from the results. By default we only remove `id` and `_state`.
    :param dry_run: convert data but don't save to Tokyo. Default is False.

    Usage::

        from pyrant import Tyrant
        from pyrant.utils.django import import_django_models

        # load your Django models
        from myapp.models import Category, Note

        database = Tyrant(host='127.0.0.1', port=1978)
        models = Category, Note
        renamings = {
            Category: {'name': 'title'},
            Note:  {'text': 'summary', 'some_legacy_field: None},
        }

        import_django_models(database, models, renamings, dry_run=True)

    """
    for model in models:
        print 'converting all %(count)d %(model)s objects...' % {
            'model': model._meta.object_name,
            'count': model.objects.count(),
        }
        model_renamings = renamings.get(model, None)
        for instance in model.objects.all():
            key = make_obj_pk(instance)
            prepared_dict = prepare_django_object(instance, model_renamings)
            if not dry_run:
                database[key] = prepared_dict
