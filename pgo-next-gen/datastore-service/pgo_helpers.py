import datadb
import parsedatetime
import time
from datetime import datetime
host_cache = None


def refresh_host_cache():
    global host_cache
    data = datadb.execute('select * from hosts')
    host_cache = data


def ensure_host_id(host_id_or_ui_shortname):
    host_id = None

    if not host_cache:
        refresh_host_cache()

    if host_id_or_ui_shortname.isdigit():
        host_id = int(host_id_or_ui_shortname)
    else:
        for x in host_cache:
            if x['host_ui_shortname'] == host_id_or_ui_shortname:
                return x['host_id']
        # maybe was added recently, refresh cache
        refresh_host_cache()
    if not host_id:
        raise Exception('unknown host_id: ' + host_id_or_ui_shortname)
    return host_id


def ensure_host_ids(host_id_or_ui_shortnames):
    return [ensure_host_id(x) for x in host_id_or_ui_shortnames]


def enusure_datetime(humanreadable_or_datetime):
    if humanreadable_or_datetime.startswith('-'):
        cal = parsedatetime.Calendar()
        p = cal.parse(humanreadable_or_datetime)
        return datetime.fromtimestamp(time.mktime(p[0]))
    else:
        return humanreadable_or_datetime    # TODO validate


def ensure_list(host_ids, data_type=int):
    if type(host_ids) == list:
        return [data_type(x) for x in host_ids]
    if not host_ids:
        return []
    return [data_type(x) for x in host_ids.split(',')]

if __name__ == '__main__':
    # print(ensure_host_id('1'))
    # print(ensure_host_id('pg'))
    # print(ensure_host_ids(['pg', 'pg']))
    # print(enusure_datetime('-1h'))
    print(ensure_list('1,2'))