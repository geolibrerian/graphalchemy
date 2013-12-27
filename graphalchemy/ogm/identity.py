from graphalchemy.ogm.state import InstanceState


class IdentityMap(dict):

    def __init__(self):
        pass


    def add(self, obj):
        if obj in self:
            return self
        state = InstanceState(obj)
        dict.__setitem__(self, obj, state)


    def get_by_id(self, id):
        if id is None:
            return None
        for obj, state in self.iteritems():
            if obj.id == id:
                return obj
        return None

