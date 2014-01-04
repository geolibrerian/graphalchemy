#! /usr/bin/env python
#-*- coding: utf-8 -*-

# ==============================================================================
#                                      IMPORTS
# ==============================================================================

# System
import importlib

# Services
from graphalchemy.ogm.identity import IdentityMap
from graphalchemy.ogm.unitofwork import UnitOfWork
from graphalchemy.ogm.repository import Repository
from graphalchemy.ogm.query import ModelAwareQuery


# ==============================================================================
#                                     SERVICE
# ==============================================================================

class OGM(object):
    """ The OGM articulates all services in one central object so the end-user
    can quickly access them. Notably, it acts as a factory for repositorys, and
    as a proxy for the current session.
    """

    def __init__(self, client, model_paths=[], logger=None):
        self.logger = logger
        self.client = client
        module = importlib.import_module(model_paths[0])
        self.metadata = module.__dict__.get('metadata')
        self._session = None
        self.repositorys = {}

    def repository(self, model_name):
        """ Returns the repository corresponding to the requested model.
        """
        if model_name in self.repositorys:
            return self.repositorys[model_name]
        model = self.metadata.for_model_name(model_name)
        class_ = self.metadata.for_model(model)
        repository = Repository(self.get_session(), model, class_, logger=self.logger)
        self.repositorys[model_name] = repository
        return repository

    def add(self, instance):
        return self.get_session().add(instance)

    def delete(self, instance):
        return self.get_session().delete(instance)

    def commit(self):
        return self.get_session().commit()

    def close(self):
        self.get_session().clear()
        self._session = None
        return self

    def get_session(self):
        if self._session is None:
            self._session = Session(
                client=self.client,
                metadata=self.metadata,
                logger=self.logger
            )
        return self._session

    def query(self, groovy, params):
        query = ModelAwareQuery(self.get_session())
        query.execute_raw_groovy(groovy, params)
        return query



class Session(object):
    """ Defines a session where a set of modifications will happen. A session
    defines which entities will be synchronized with the database. Such
    modifications will happen grouped in a unitofwork when the commit() method
    is called.
    """

    def __init__(self, client, metadata, logger=None):
        self.identity_map = IdentityMap()
        self.metadata_map = metadata
        self.client = client
        self.logger = logger

        self._add = []
        self._delete = []


    def add(self, instance):
        """ Adds an instance to the session, so it is updated / inserted at
        the next commit.
        :param instance: The instance to add to the session.
        :type instance: graphalchemy.blueprints.schema.Model
        :returns: This object itself.
        :rtype: graphalchemy.ogm.session.Session
        """
        if instance in self._delete:
            self._delete.removeitem(instance)
        if instance in self._add:
            self._log('Instance already tracked.')
            return self
        self._add.append(instance)
        return self


    def delete(self, instance):
        """ Schedules an instance for deletion, so it is deleted at the
        next commit.
        :param instance: The instance to delete in the session.
        :type instance: graphalchemy.blueprints.schema.Model
        :returns: This object itself.
        :rtype: graphalchemy.ogm.session.Session
        """
        if instance not in self.identity_map:
            raise Exception('Object is not in the identity map.')
        if instance in self._add:
            self._add.removeitem(instance)
        if instance in self._delete:
            self._log('Instance already scheduled for delete.')
            return self
        self._delete.append(instance)
        return self


    def clear(self):
        """ Clears the current session.
        :returns: This object itself.
        :rtype: graphalchemy.ogm.session.Session
        """
        self.identity_map.clear()
        self._delete = []
        self._add = []
        return self


    def commit(self):
        """ Performs all changes scheduled in the current session, grouped in
        a UnitOfWork.
        :returns: This object itself.
        :rtype: graphalchemy.ogm.session.Session
        """

        uow = UnitOfWork(self.client, self.identity_map, self.metadata_map, logger=self.logger)

        # We need to save/update nodes first
        for obj in self._add:
            if self.metadata_map.is_node(obj):
                uow.register_object(obj, 'add')
                self._log("Inserted "+str(obj))
        for obj in self._add:
            if self.metadata_map.is_relationship(obj):
                uow.register_object(obj, 'add')
                self._log("Inserted "+str(obj))

        # We need to delete relations first
        for obj in self._delete:
            if self.metadata_map.is_relationship(obj):
                uow.register_object(obj, 'delete')
                self._log("Deleted "+str(obj))
        for obj in self._delete:
            if self.metadata_map.is_node(obj):
                uow.register_object(obj, 'delete')
                self._log("Deleted "+str(obj))
        self._delete = []

        return self


    def _log(self, message, level=10):
        if self.logger is None:
            return self
        self.logger.log(level, message)
        return self


    def get_vertex(self, id):
        obj = self.identity_map.get_by_id(id)
        if obj:
            return obj, False
        return self.client.get_vertex(id), True

