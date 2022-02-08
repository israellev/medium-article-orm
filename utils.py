class Utils:

    @staticmethod
    def MergeTwoDicts(x, y):
        z = x.copy()
        z.update(y)
        return z

    @staticmethod
    def GetClassListValues(cls):
        return [getattr(cls, key) for key in dir(cls) if not key.startswith('__')]

    @staticmethod
    def IsList(List):
        return isinstance(List, (list, tuple))
