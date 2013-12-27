#! /usr/bin/env python
#-*- coding: utf-8 -*-

# ==============================================================================
#                                      IMPORTS
# ==============================================================================

from graphalchemy.ogm.query import ModelAwareQuery


# ==============================================================================
#                                     SERVICE
# ==============================================================================

class Repository(object):
    """ Repositories represent collections of objects that follow a given
    model. As such, they rely both on the Python class of the model and on
    the model itself. As such, they contain :

    Methods to create an object :
    >>> website = repository(domain="http://www.foodnetwork.com")
    >>> website = repository.create(domain="http://www.allrecipes.com")

    Methods to query this collection of objects
    >>> users = repository.filter(firstname="Joe")
    >>> users = repository.filter(firstname="Joe", lastname="Miller")

    Repositories can be overloaded by the user to group shortcut queries that
    are usefull for his own purpose.
    >>> users = repository.find_new()

    Repositories can be loaded directly from the OGM :
    >>> repository = ogm.repository('User')
    """

    def __init__(self, session, model, class_, logger=None):
        """ Loads a repository.

        :param session: The session to perform requests against.
        :type session: graphalchemy.ogm.session.Session
        :param model: The model instance of the current class.
        :type model: graphalchemy.blueprints.schema.Model
        :param class_: The class that is actually mapped.
        :type class_: object
        :param logger: An optionnal logger.
        :type logger: logging.Logger
        """
        self.session = session
        self.model = model
        self.class_ = class_
        self.logger = logger


    def __call__(self, *args, **kwargs):
        """ Creates an instance of the mapped class, initialized with the
        given parameters.

        Example use:
        >>> website = repository(domain="http://www.foodnetwork.com")

        :param class_: The class that is actually mapped.
        :type class_: object
        """
        return self.create(*args, **kwargs)


    def create(self, *args, **kwargs):
        """ Creates an instance of the mapped class, initialized with the
        given parameters.

        Example use :
        >>> website = repository.create(domain="http://www.allrecipes.com")

        :param class_: The class that is actually mapped.
        :type class_: object
        """
        return self.class_(*args, **kwargs)


    def get(self, id):
        """ Retrieves an element from its id.

        Example use :
        >>> website = repository.get(123)

        :param id: The element id.
        :type id: int
        :returns: The object with the given id in the database.
        :rtype: object
        """

        # Retrieve from DB or identity map
        response, loaded = self.session.get_vertex(id)
        if not loaded:
            self._log('Object found in entity map')
            return response
        self._log('Object not found in entity map')
        results = response.content['results']

        # Verify id
        result = ModelAwareQuery(self.session).vertices() \
                                              .filter(eid=id) \
                                              .one()

        # @todo
        # add a check model

        if int(result.id) != int(id):
            raise Exception('Expected '+str(id)+', got '+str(result.id ))

        return result



    def filter(self, **kwargs):
        """ We have to pre-process the query here to use the right index.
        """
        query = ModelAwareQuery(self.session).vertices()
        # If one of the arguments is indexed, we use it first.
        indices = self.model._useful_indices_among(kwargs)
        if len(indices):
            index_name = indices[0]
            key = index_name
            value = kwargs[index_name]
            query.filter_on_index(index_name, key, value)
            kwargs.pop(index_name)
        # Else, we simply use the index on the model name.
        else:
            index_name = self.model.model_name_storage_key
            key = index_name
            value = self.model.model_name
            query.filter_on_index(index_name, key, value)

        # Rename filter keys if the db_name is not the same
        filters = {}
        for name_py, value in kwargs.iteritems():
            prop = self.model._properties.get(name_py, None)
            if prop is None:
                raise Exception('Property %s not found in model %s' % (name_py, prop, ))
            filters[prop.name_db] = value

        query.filter(**filters)
        return query


    def _build_object(self, results):
        obj = self.class_(results)
        self._update_object(obj, results)
        return obj


    def _update_object(self, obj, results):
        for property_db, value_db in results.iteritems():
            found = False
            for property in self.model._properties.values():
                if property.name_db != property_db:
                    continue
                found = True
                break
            if not found:
                raise Exception('Property retrieved but not found : '+property_db)
            value_py = property.to_py(value_db)
            setattr(obj, property.name_py, value_py)
        return obj


    def _check_model_name(self, model_name):
        if model_name != self.model.model_name:
            raise Exception('Expected vertex, got '+str(model_name))
        return True


    def _check_type(self, _type):
        if _type == 'vertex' and self.model.is_node():
            return True
        elif _type == 'edge' and self.model.is_relationship():
            return True
        raise Exception('Received '+_type+' for '+self.model.model_type)


    def _log(self, message, level=10):
        """ Thin wrapper for logging purposes.

        :param message: The message to log.
        :type message: str
        :param level: The level of the log.
        :type level: int
        :returns: This object itself.
        :rtype: graphalchemy.blueprints.schema.Validator
        """
        if self.logger is not None:
            self.logger.log(level, message)
        return self