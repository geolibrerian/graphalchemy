#! /usr/bin/env python
#-*- coding: utf-8 -*-

# ==============================================================================
#                                      IMPORTS
# ==============================================================================

from urllib import quote
from urllib import quote_plus
from urllib import urlencode

from bulbs.gremlin import Gremlin

# ==============================================================================
#                                      EXCEPTIONS
# ==============================================================================

class NoResultFound(Exception):
    pass

class MultipleResultsFound(Exception):
    pass


# ==============================================================================
#                                      SERVICE
# ==============================================================================

class Query(object):

    EDGE = 'edge'
    VERTEX = 'vertex'

    def __init__(self, session, *args, **kwargs):

        # Clients
        self.session = session
        self.gremlin = Gremlin(self.session.client)

        # Query definition
        self._on = None
        self._filters = {}
        self._indices = {}
        self._offset = None
        self._limit = None

        # Results
        self._results = None

        self.logger = kwargs.get('logger', None)


    def edges(self):
        """ Specifies that the query is made on edges.

        Example:
        >>> query.edges().filter('createdAt', date).execute()
        gremlin> g.E.has('createdAt', date)

        :returns: This object itself.
        :rtype: graphalchemy.ogm.query.Query
        """
        self._on = self.EDGE
        return self

    def vertices(self):
        """ Specifies that the query is made on vertices.

        Example:
        >>> query.vertices().filter('name', 'Foo').execute()
        gremlin> g.V.has('name', 'Foo')

        :returns: This object itself.
        :rtype: graphalchemy.ogm.query.Query
        """
        self._on = self.VERTEX
        return self



    def filter(self, **kwargs):
        """ Performs a filtering operation on the given repository. Automaticaly
        decides which index to use :
        - the eid index if provided
        - the first property key index that is matched
        Will add extra filtering as simple as queries.

        Example :
        >>> iterator = repository.filter(domain='http://www.foo.com', name='Foo')
        >>> iterator = repository.filter(eid=123)

        @todo : it should return an iterator and wait for extra filtering.
        @todo : only limited to filtering on one entity for now.

        :returns: This object itself.
        :rtype: graphalchemy.ogm.query.Query
        """
        self._filters.update(kwargs)
        return self


    def filter_on_index(self, index_name, key, value):
        """ Performs a filter based on an index.

        Note that in Titan, indexed are based on the key names, so most of the
        time, index_name will be the same as key.

        :param index_name: The name of the index to use.
        :type index_name: str
        :param key: The key to search for in the index.
        :type key: str
        :param value: The value to match the key to.
        :type value: mixed
        :returns: This object itself.
        :rtype: graphalchemy.ogm.query.Query
        """
        self._indices[index_name] = {"key": key, "value":value}
        return self


    def _compile_groovy(self):
        """ Builds the Groovy Gremlin statement that corresponds to this query.
        @todo: For now, this only work on a single node or relationship.

        :returns: The gremlin query.
        :rtype: string, dict
        """

        if self._on is None:
            raise Exception('Type of query not specified.')

        query = 'g'
        params = {}
        started = False

        # If the id is in the parameters :
        if 'eid' in self._filters:
            if self._on == self.EDGE:
                query += '.e'
            elif self._on == self.VERTEX:
                query += '.v'
            query += '(eid)'
            params['eid'] = self._filters['eid']
            self._filters.pop('eid')
            started = True

        if self._on == self.EDGE:
            prefix = '.E'
        elif self._on == self.VERTEX:
            prefix = '.V'

        # If one of the parameters is indexed :
        if self._indices:
            for index, value in self._indices.items():
                query += prefix + '("'+value['key']+u'", '+value['key']+u')'
                params[value['key']] = value['value']
                started = True
                # For now, we can only query on one index
                break

        if started == False:
            query += prefix

        # Fillup with remaining filters
        for key, value in self._filters.iteritems():
            query += '.has("'+key+u'", '+key+u')'
            params[key] = value

        return query, params


    def _compile_rexster(self, **kwargs):
        """ Builds a gremlin query string from a set of filtering arguments.
        @todo : no escaping is implemented yet.

        :returns: string, dict -- The gremlin query.
        """
        def clean(value):
            return quote(unicode(value), safe='')

        path = ''

        # If the id is in the parameters :
        if 'id' in kwargs:
            path = '/vertices/'+clean(kwargs['id'])
            return path

        # If one of the parameters is indexed :
        if self._indices:
            for index, value in self._indices.items():
                path += '/indices/'+clean(index)+u'?key='+clean(value['key'])+u'&value='+clean(value['value'])
                # For now, we can only query on one index
                break
            # For now, we can only handle one index
            return path

        # If no parameter is indexed :
        if self.model.is_node():
            path += '/vertices'
        else:
            path += '/edges'

        # Fillup with remaining filters
        for key, val in kwargs.iteritems():
            path += u'?key='+clean(key)+u'&value='+clean(val)
            # For now, Rexster seems to support only one key/value query
            break

        return path


    def compile(self):
        """ Compiles the current request by choosing the best method (Rexster
        or Gremlin) and resets all parameters.
        """
        # Rexster :
        # path = self._compile_rexster()

        # Groovy
        query, params = self._compile_groovy()

        # Reset
        self._filters = {}
        self._indices = {}
        self._on = None

        return query, params


    def execute(self):
        script, params = self.compile()
        return self.execute_raw_groovy(script, params)


    def execute_raw_groovy(self, query, params={}):
        response = self.gremlin.execute(query, params=params)
        self._results = response.content['results']
        return self


    def execute_raw_rexster(self, query, params={}):
        response = self.session.client.request.get(query, params=params)
        self._results = response.results
        return self


    def all(self):
        """Return the results represented by this Query as a list.
        This results in an execution of the underlying query.
        """
        return list(self)


    def one(self):
        """Return exactly one result or raise an exception.

        Raises ``sqlalchemy.orm.exc.NoResultFound`` if the query selects
        no rows.  Raises ``sqlalchemy.orm.exc.MultipleResultsFound``
        if multiple object identities are returned, or if multiple
        rows are returned for a query that does not return object
        identities.

        Calling one() results in an execution of the underlying query.
        """
        ret = list(self)

        l = len(ret)
        if l == 1:
            return ret[0]
        elif l == 0:
            raise NoResultFound("No row was found for one()")
        else:
            raise MultipleResultsFound("Multiple rows were found for one()")


    def first(self):
        """Return the first result of this Query or None if the result doesn't
        contain any row.

        Calling ``first()`` results in an execution of the underlying query.
        """
        ret = list(self)[0:1]
        if len(ret) > 0:
            return ret[0]
        else:
            return None


    def slice(self, start, stop):
        """apply LIMIT/OFFSET to the ``Query`` based on a "
        "range and return the newly resulting ``Query``."""

        if start is not None and stop is not None:
            self._offset = (self._offset or 0) + start
            self._limit = stop - start
        elif start is None and stop is not None:
            self._limit = stop
        elif start is not None and stop is None:
            self._offset = (self._offset or 0) + start

        if self._offset == 0:
            self._offset = None


    def limit(self, limit):
        """Apply a ``LIMIT`` to the query and return the newly resulting
        ``Query``.
        """
        self._limit = limit
        return self


    def offset(self, offset):
        """Apply an ``OFFSET`` to the query and return the newly resulting
        ``Query``.
        """
        self._offset = offset
        return self


    def __iter__(self):
        if self._results is None:
            self.execute()
        for result in self._results:
            yield result


    def _log(self, message, level=10):
        """ Thin wrapper for logging purposes.

        :param message: The message to log.
        :type message: str
        :param level: The level of the log.
        :type level: int
        :returns: This object itself.
        :rtype: graphalchemy.ogm.query.Query
        """
        if self.logger is not None:
            self.logger.log(level, message)
        return self



class ModelAwareQuery(Query):

    def __init__(self, session, *args, **kwargs):
        self.metadata_map = session.metadata_map
        super(ModelAwareQuery, self).__init__(session, *args, **kwargs)

    def execute(self):
        super(ModelAwareQuery, self).execute()
        self.hydrate()

    def execute_raw_groovy(self, query, params={}):
        super(ModelAwareQuery, self).execute_raw_groovy(query, params=params)
        self.hydrate()


    def hydrate(self):
        for i, result in enumerate(self._results):
            self._results[i] = self._build_object(result)

    def _build_object(self, result):
        if not isinstance(result, dict):
            raise Exception('Expected dict, got '+str(result))
        # Check if not in session
        obj = self.session.identity_map.get_by_id(result.get('_id'))
        if obj:
            return obj
        return self.metadata_map._object_from_dict(result)













