#! /usr/bin/env python
#-*- coding: utf-8 -*-

# ==============================================================================
#                                      IMPORTS
# ==============================================================================

from graphalchemy.ogm.identity import IdentityMap
from graphalchemy.ogm.unitofwork import UnitOfWork
from graphalchemy.ogm.state import InstanceState

from graphalchemy.ogm.repository import Repository
from graphalchemy.ogm.query import ModelAwareQuery

# ==============================================================================
#                                     SERVICE
# ==============================================================================

class OGM(object):

    def __init__(self, client, metadata, logger=None):
        self.logger = logger
        self.client = client
        self.metadata = metadata
        self.session = Session(client=client, metadata=metadata, logger=self.logger)
        self.repositorys = {}

    def repository(self, model_name):
        if model_name in self.repositorys:
            return self.repositorys[model_name]
        model = self.metadata.for_model_name(model_name)
        class_ = self.metadata.for_model(model)
        repository = Repository(self.session, model, class_, logger=self.logger)
        self.repositorys[model_name] = repository
        return repository

    def add(self, instance):
        return self.session.add(instance)

    def delete(self, instance):
        return self.session.delete(instance)

    def commit(self):
        return self.session.commit()

    def query(self, groovy, params):
        query = ModelAwareQuery(self.session)
        query.execute_raw_groovy(groovy, params)
        return query



class Session(object):

    def __init__(self, client, metadata, logger=None):
        self.identity_map = IdentityMap()
        self.metadata_map = metadata
        self.client = client
        self.logger = logger

        self._update = []
        self._delete = []
        self._new = []


    def add(self, instance):
        if instance in self.identity_map:
            self._update.append(instance)
        else:
            self._new.append(instance)
        return self


    def delete(self, instance):
        if instance not in self.identity_map:
            raise Exception('Object is not in the identity map.')
        self._delete.append(instance)
        return self


    def get_vertex(self, id):
        obj = self.identity_map.get_by_id(id)
        if obj:
            return obj, False
        return self.client.get_vertex(id), True


    def add_to_identity_map(self, obj):
        # Add to the identity_map
        self.identity_map[obj] = InstanceState(obj)
        self.identity_map[obj].update_id(id)
        # self.identity_map[obj].update_attributes(data)
        return self


    def clear(self):
        self.identity_map.clear()
        self._update = []
        self._delete = []
        self._new = []
        return self


    def commit(self):

        uow = UnitOfWork(self.client, self.identity_map, self.metadata_map, logger=self.logger)

        # We need to save nodes first
        for obj in self._new:
            if self.metadata_map.is_node(obj):
                uow.register_object(obj, 'new')
                self._log("Inserted "+str(obj))
        for obj in self._new:
            if self.metadata_map.is_relationship(obj):
                uow.register_object(obj, 'new')
                self._log("Inserted "+str(obj))

        # Update all other nodes
        for obj in self._update:
            if self.metadata_map.is_node(obj):
                uow.register_object(obj, 'update')
                self._log("Updated "+str(obj))
        for obj in self._update:
            if self.metadata_map.is_relationship(obj):
                uow.register_object(obj, 'update')
                self._log("Updated "+str(obj))

        # We need to delete relations first
        for obj in self._delete:
            if self.metadata_map.is_relationship(obj):
                uow.register_object(obj, 'delete')
                self._log("Deleted "+str(obj))
        for obj in self._delete:
            if self.metadata_map.is_node(obj):
                uow.register_object(obj, 'delete')
                self._log("Deleted "+str(obj))

        return self


    def _log(self, message, level=10):
        if self.logger is None:
            return self
        self.logger.log(level, message)