#! /usr/bin/env python
#-*- coding: utf-8 -*-

# ==============================================================================
#                                      IMPORTS
# ==============================================================================

from graphalchemy.ogm.state import InstanceState


# ==============================================================================
#                                      SERVICE
# ==============================================================================

class IdentityMap(dict):
    """ Keeps a map of all entities that are currently loaded in a session, in
    order to be able to return them directly without querying the database if
    they are requested. Also holds references to virtual edges, relations, etc.

    Implements the interface of a standard dict, where keys are tracked entities
    and values are the corresponding state.
    """

    def add(self, obj, update=False):
        """ Adds an object to the identity map. If the object is not known, creates
        a fresh InstanceState.
        """
        if obj in self:
            return self
        state = InstanceState(obj)
        if update:
            state.update_id(obj.id)
            state.update_attributes(data)
        return super(IdentityMap, self).__setitem__(obj, state)


    def get_by_id(self, id):
        """ Returns an entity given its id. In graph databases, the id is unique
        across all entities.

        :param id: The numerical identifier of the entity to retrieve.
        :type id: int
        """
        if id is None:
            return None
        for obj, state in self.iteritems():
            if obj.id == id:
                return obj
        return None

