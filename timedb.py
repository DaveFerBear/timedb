'''

A simple time-based, columnar data store.

'''

import time
import os


class TimeDB(object):
    def __init__(self, folder='./store'):
        self.folder = folder
        self.n = 0
        self.field_ptr = {}
        self._clean_dir(folder)

    def add(self, data):
        data['_timestamp'] = time.monotonic_ns()
        cur_dirs = [_ for _ in os.listdir(self.folder)]
        self.n = self.n + 1
        for field in data:
            # Update the field counts
            if field not in self.field_ptr:
                self.field_ptr[field] = 0
            self.field_ptr[field] = self.field_ptr[field] + 1

            # Creates file if it doesn't exist. Opens in append mode.
            with open('{}/{}'.format(self.folder, field), 'a+') as f:
                if self.field_ptr[field] < self.n:
                    f.writelines(['\n'] * (self.n - self.field_ptr[field]))
                    self.field_ptr[field] = self.n
                f.write(str(data[field]) + '\n')  # Is newline needed?

    def remove(self, data):
        pass

    def query(self, filter):
        pass

    def _read_file(self, fname):
        with open(fname, 'r') as f:
            while f.readline():
                pass

    def _write_file(self, fname, data):
        with open(fname, 'w') as f:
            f.write(data)

    def _clean_dir(self, dir):
        for f in os.listdir(dir):
            os.remove('{}/{}'.format(dir, f))


data_in = [
    {
        'path': '/user',
        'time': 23,
        'status': 200
    },
    {
        'path': '/user/123',
        'time': 80,
        'status': 400,
        'error': 'Invalid input.'
    },
    {
        'path': '/org',
        'time': 101,
        'status': 200
    },
    {
        'path': '/user/123',
        'time': 101,
        'status': 404,
        'error': 'User not found'
    },
    {'retries': 3}
]

tdb = TimeDB()

for din in data_in:
    tdb.add(din)
