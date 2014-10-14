from cherrypy import HTTPError
from datetime import date, time, datetime
from json import JSONEncoder
from decimal import Decimal


from export import Export


"""
Paths
-----

The paths to the existing endpoints are listed in the following table, along with a list of the parameters they take
(optional parameters are in square brackets), and information regarding whether or not they actually work.

----------------------------------- -------------------------------- ----------
 path                                parameters                       working
----------------------------------- -------------------------------- ----------
 /api/                                                                maybe
 /api/host/                          host, [limit]                    probably
 /api/hosts/                                                          probably
 /api/indexes/                                                        no
 /api/indexes/performance/           host                             maybe
 /api/load/overview/                 [host]                           probably
 /api/load/overview4w/                                                no
 /api/locks/blocking/                [from_date], [to_date]           maybe
 /api/queries/performance/           host, [from_date], [to_date]     probably
 /api/schemas/unused/                [host], [from_date], [to_date]   probably
 /api/sprocs/                        host                             probably
 /api/sprocs/performance/            host, from_version, to_version   maybe
 /api/sprocs/top/                    by                               probably
 /api/tables/                                                         no
 /api/tables/all/                    host, [order]                    probably
 /api/tables/performance/            host, from_date, to_date         maybe
----------------------------------- -------------------------------- ----------
"""


class Root(object):

    def __init__(self, webroot):
        self.webroot = webroot
        self.host = Host(webroot)
        self.hosts = Hosts(webroot)
        self.indexes = self.indices = Indexes(webroot)
        self.load = Load(webroot)
        self.locks = Locks(webroot)
        self.queries = Queries(webroot)
        self.schemas = Schemas(webroot)
        self.sprocs = Sprocs(webroot)
        self.tables = Tables(webroot)
        self.bloat = Bloat(webroot)
        self.v1 = self.v1_0 = self

    def index(self):
        return jsonify(self.webroot.get_last_loads_and_sizes())

    index.exposed = True


class Host(object):

    def __init__(self, webroot):
        self.webroot = webroot

    def __call__(self, host, limit=10):
        if hasattr(self.webroot, host):
            return jsonify(getattr(self.webroot, host).raw(limit))
        else:
            raise HTTPError(404)

    exposed = True


class Hosts(object):

    def __init__(self, webroot):
        self.webroot = webroot

    def index(self):
        return jsonify(self.webroot.hosts.raw())

    index.exposed = True


class Indexes(object):

    def __init__(self, webroot):
        self.webroot = webroot

    def default(self, host, table, from_date=None, to_date=None):
        return jsonify(self.webroot.indexes.raw(host, table, from_date, to_date))

    def performance(self, host):
        return jsonify(self.webroot.perfindexes.raw(host))

    default.exposed = True
    performance.exposed = True


class Load(object):

    def __init__(self, webroot):
        self.webroot = webroot

    def overview(self, host=None):
        return jsonify(self.webroot.report.raw(host))

    overview.exposed = True


class Locks(object):

    def __init__(self, webroot):
        self.webroot = webroot

    def blocking(self, from_date=None, to_date=None):
        return jsonify(self.webroot.perflocks.raw(from_date, to_date))

    blocking.exposed = True


class LogFiles(object):

    def __init__(self, webroot):
        self.webroot = webroot

    # not used


class Queries(object):

    def __init__(self, webroot):
        self.webroot = webroot

    def performance(self, host, from_date=None, to_date=None):
        return jsonify(self.webroot.perfstatstatements.raw(host, from_date, to_date))

    performance.exposed = True


class Schemas(object):

    def __init__(self, webroot):
        self.webroot = webroot

    def unused(self, host='all', from_date=None, to_date=None):
        return jsonify(self.webroot.perfschemas.raw(host, from_date, to_date))

    unused.exposed = True


class Sprocs(object):

    def __init__(self, webroot):
        self.webroot = webroot

    def __call__(self, host):
        return jsonify(self.webroot.sprocs.get_data(host))

    def top(self, by):
        if by in ('calls', 'by-calls'):
            return self.webroot.export.topsprocsbycalls()
        elif by in ('runtime', 'by-runtime'):
            return self.webroot.export.topsprocsbyruntime()
        else:
            raise HTTPError(400)

    def performance(self, host, from_version, to_version):
        return jsonify(self.webroot.perfapi.raw(host, from_version, to_version))

    exposed = True
    top.exposed = True
    performance.exposed = True


class Tables(object):

    def __init__(self, webroot):
        self.webroot = webroot

    def index(self):
        return jsonify(self.webroot.tables.get_data())

    def all(self, host, order=None):
        return jsonify(self.webroot.tables.raw_alltables(host, order))

    def performance(self, host, from_date, to_date):
        return jsonify(self.webroot.perftables.raw(host, from_date, to_date))

    index.exposed = True
    all.exposed = True
    performance.exposed = True


class Bloat(object):

    def __init__(self, webroot):
        self.webroot = webroot

    def tables(self, host, **params):
        params['bloat_type'] = 'table'
        return jsonify(self.webroot.perfbloat.raw(host, **params))

    def indexes(self, host, **params):
        params['bloat_type'] = 'index'
        return jsonify(self.webroot.perfbloat.raw(host, **params))

    tables.exposed = True
    indexes.exposed = True




### ENCODER ###

class DateEncoder(JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime):
            return "{} {}".format(obj.date(), str(obj.time())[:8]) # no microseconds
        if isinstance(obj, date) or isinstance(obj, time):
            return str(obj)[:8]
        if isinstance(obj, Decimal):
            return str(obj)
        return super(self.__class__, self).default(obj)

jsonify = DateEncoder().encode

