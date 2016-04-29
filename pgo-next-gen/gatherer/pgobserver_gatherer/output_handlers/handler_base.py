from multiprocessing import Process
from multiprocessing import Queue


class HandlerBase(Process):

    def __init__(self, name, settings=None, filters=None):
        Process.__init__(self)
        self.daemon = True
        self.queue = Queue()    # items are tuple of (hostname, dataset_name, data_as_list_of_dicts)
        self.name = name
        self.settings = settings if settings else {}
        self.filters = filters if filters else []

    def run(self):
        raise Exception('Subclasses should implement an infinite loop consuming the queue')
