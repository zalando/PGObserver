import logging
import time
from collections import defaultdict

from pgobserver_gatherer.output_handlers.handler_base import HandlerBase


class Handler(HandlerBase):

    def __init__(self, settings=None, filters=None):    # TODO configurable file names
        self.name = 'filehandler'
        super().__init__(self.name, settings, filters)
        self.out_files = defaultdict(dict)     # every dataset gets its own outfile
        logging.info('[%s] initialized. settings: %s, filters: %s', self.name, settings, filters)

    def run(self):
        logging.debug('[%s] starting', self.name)
        logging.debug('[%s] queue=%s', self.name, self.queue)

        while True:
            logging.debug('[%s] qsize() %s', self.name, self.queue.qsize())
            if self.queue.qsize() > 0:
                host_name, dataset_name, data = self.queue.get()
                logging.debug('[%s] got dataset %s with len. %s', self.name, dataset_name, len(data))
                if data:
                    self.print_dataset_to_file(host_name, dataset_name, data)
            else:
                logging.debug('[%s] sleeping', self.name)
                time.sleep(1)

    def print_dataset_to_file(self, host_name, dataset_name, data):

        if dataset_name not in self.out_files[host_name]:
            f = self.out_files[host_name][dataset_name] = open('{}_{}_console.log'.format(host_name, dataset_name), 'a')
            print('\t\t'.join(sorted(data[0].keys())), file=f)    # TODO re-use format from "console" plugin
        else:
            f = self.out_files[host_name][dataset_name]

        for d in data:
            ds = sorted(d.items(), key=lambda x: x[0])
            print('\t\t'.join([str(y) for x, y in ds]), file=f)
        f.flush()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    h = Handler()
    h.start()
    print('handler started')
    print(h)
    print('q', h.queue)
    for i in range(1, 5):
        print('putting', {'k1': i})
        h.queue.put(('set1', {'k1': i}))
        print('items', h.queue.qsize())
        time.sleep(1)
