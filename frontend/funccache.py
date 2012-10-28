'''
Created on Sep 19, 2011

@author: jmussler
'''
from __future__ import print_function
import collections
import functools
import time

def lru_cache(lifetime=60,maxsize=100):
    '''Least-recently-used cache decorator.

    Arguments to the cached function must be hashable.
    Cache performance statistics stored in f.hits and f.misses.
    http://en.wikipedia.org/wiki/Cache_algorithms#Least_Recently_Used

    '''
    def decorating_function(user_function):
        cache = collections.OrderedDict()    # order: least recent to most recent

        @functools.wraps(user_function)
        def wrapper(*args, **kwds):
            key = args
            if kwds:
                key += tuple(sorted(kwds.items()))
            try:
                result = cache.pop(key)
                if(result['validTo']<time.time()):
                    result = user_function(*args, **kwds)
                    print ( "found too old entry" )
                    wrapper.misses += 1
                else:
                    print ( "found entry" )
                    wrapper.hits += 1                
                    result = result['result']
                    
            except KeyError:
                result = user_function(*args, **kwds)
                wrapper.misses += 1
                if len(cache) >= maxsize:
                    cache.popitem(0)
                    
            cache[key] = { 'result' : result, 'validTo': time.time()+lifetime }
            
            return result
        
        wrapper.hits = wrapper.misses = 0
        
        return wrapper
    
    return decorating_function
