from multiprocessing.managers import NamespaceProxy, BaseManager
from pandas import DataFrame
import inspect
import os

class MyDataFrame(DataFrame):
    def __getstate__(self):
        print(f'dataframe being pickled in pid {os.getpid()}')
        return super().__getstate__()

    def __setstate__(self, state):
        print(f'dataframe being unpickled in pid {os.getpid()}')
        print()
        return super().__setstate__(state)


class ObjProxy(NamespaceProxy):
    """Returns a proxy instance for any user defined data-type. The proxy instance will have the namespace and
    functions of the data-type (except private/protected callables/attributes). Furthermore, the proxy will be
    pickable and can its state can be shared among different processes. """

    @classmethod
    def populate_obj_attributes(cls, real_cls):
        DISALLOWED = set(dir(cls))
        ALLOWED = ['__sizeof__', '__eq__', '__ne__', '__le__', '__repr__', '__dict__', '__lt__',
                   '__gt__']
        DISALLOWED.add('__class__')
        new_dict = {}
        for (attr, value) in inspect.getmembers(real_cls, callable):
            if attr not in DISALLOWED or attr in ALLOWED:
                new_dict[attr] = proxy_wrap(attr)
        return new_dict


def proxy_wrap(attr):
    """ This method creates function that calls the proxified object's method."""
    def f(self, *args, **kwargs):

        # _callmethod is the method that proxies provided by multiprocessing use to call methods in the proxified object
        return self._callmethod(attr, args, kwargs)

    return f