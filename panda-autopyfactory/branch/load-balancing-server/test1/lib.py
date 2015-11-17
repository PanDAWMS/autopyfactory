class Singleton(type):
    def __init__(cls, name, bases, dct):
        cls.__instance = None
        type.__init__(cls, name, bases, dct)
    def __call__(cls, *args, **kw):
        if cls.__instance is None:
            cls.__instance = type.__call__(cls, *args,**kw)
        return cls.__instance


class C:
        __metaclass__ = Singleton

        def __init__(self):
                # data is the JSON structure with info
                self.data = None


        def add(self, data):

        def get(self):


