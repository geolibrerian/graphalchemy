#! /usr/bin/env python
#-*- coding: utf-8 -*-

# ==============================================================================
#                                      IMPORTS
# ==============================================================================


# ==============================================================================
#                                     SERVICE
# ==============================================================================

class UnitOfWork(object):

    def __init__(self, client, identity_map, metadata_map, logger=None):
        self.client = client
        self.identity_map = identity_map
        self.metadata_map = metadata_map
        self.logger = logger


    def register_object(self, obj, state):
        if state == 'add':
            self.register_object_add(obj)
        elif state == 'delete':
            self.register_object_delete(obj)
        else:
            raise Exception()
        return self


    def register_object_delete(self, obj):

        if not hasattr(obj, 'id'):
            raise Exception('Object has no id.')

        class_meta = self.metadata_map.for_object(obj)
        if class_meta.is_node():
            response = self.client.delete_vertex(obj.id)
            self._log("Deleted node %i" % (obj.id, ))
        elif class_meta.is_relationship():
            response = self.client.delete_edge(obj.id)
            self._log("Deleted edge %i" % (obj.id, ))

        return self


    def register_object_add(self, obj):
        if obj not in self.identity_map:
            self.register_object_insert(obj)
            self._log("Not found in identity map : inserting.")
        else:
            self.register_object_update(obj)
            self._log("Found in identity map : updating.")
        return self


    def register_object_update(self, obj):

        class_meta = self.metadata_map.for_object(obj)
        identity = self.identity_map[obj]

        # Get data to update
        data = {}
        for property in class_meta._properties.values():
            python_value = getattr(obj, property.name_py)
            property.validate(python_value)
            if identity.attribute_has_changed(property.name_py, python_value):
                data[property.name_db] = property.to_db(python_value)
                self._log('  Property '+str(property)+' changed to '+str(python_value)+', updating.')
            else:
                self._log('  Property '+str(property)+' has not changed.')

        # Update
        if not len(data):
            self._log("Nothing to update in "+str(identity.id))
            return self

        if class_meta.is_node():
            response = self.client.update_vertex(identity.id, data)
            print response.content
            self._log("Updated node "+str(identity.id))
        elif class_meta.is_relationship():
            response = self.client.update_edge(identity.id, data)
            self._log("Updated edge "+str(identity.id))

        return self


    def register_object_insert(self, obj):

        class_meta = self.metadata_map.for_object(obj)

        # Get data to update
        data = {}
        for property in class_meta._properties.values():
            self._log('  Property '+str(property)+' is new.')
            python_value = getattr(obj, property.name_py)
            property.validate(python_value)
            data[property.name_db] = property.to_db(python_value)
        data[class_meta.model_name_storage_key] = class_meta.model_name

        # Insert
        index_name = ''
        if class_meta.is_node():
            response = self.client.create_vertex(data)
        elif class_meta.is_relationship():
            data.pop('label')
            response = self.client.create_edge(
                obj.outV.id,
                class_meta.model_name,
                obj.inV.id,
                data
            )

        # Update identity map
        id = response.content['results']['_id']
        self._log('  Property '+str('id')+' updated to '+str(id))
        obj.id = id
        self.identity_map.add(obj, update=True)
        return self


    def _log(self, message, level=10):
        if self.logger is None:
            return self
        self.logger.log(level, message)