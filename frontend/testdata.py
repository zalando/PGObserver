#!/usr/bin/python
import psycopg2
import psycopg2.extras
import json
import os.path
import random

from argparse import ArgumentParser

def create_host(conn):
    cur = conn.cursor()
    cur.execute("""INSERT INTO hosts(host_id,host_name,host_enabled,host_settings) VALUES(1111,'test-host',true,
                                  '{"uiShortName":"test-server",
                                    "uiLongName":"Test Server",
                                    "loadGatherInterval": 0,
                                    "tableIoGatherInterval": 0,
                                    "sprocGatherInterval": 0,
                                    "tableStatsGatherInterval": 0 }')""")
    cur.close()

def clear_test_data(conn):
    print "Clearing test data..."
    cur = conn.cursor()

    cur.execute("DELETE FROM sproc_performance_data WHERE sp_sproc_id IN ( SELECT sproc_id FROM sprocs WHERE sproc_host_id = 1111 )")
    cur.execute("DELETE FROM sprocs WHERE sproc_host_id = 1111")

    cur.execute("DELETE FROM table_size_data WHERE tsd_table_id IN ( SELECT t_id FROM tables WHERE t_host_id = 1111 )")
    cur.execute("DELETE FROM table_io_data WHERE tio_table_id IN ( SELECT t_id FROM tables WHERE t_host_id = 1111 ) ")
    cur.execute("DELETE FROM tables WHERE t_host_id = 1111 ")
    cur.execute("DELETE FROM host_load WHERE load_host_id = 1111")

    cur.execute("DELETE FROM hosts WHERE host_id = 1111")

    cur.close()

def create_n_sprocs(conn,n):
    print "Creating stored procedures..."
    cur = conn.cursor()
    i = 0
    while i < n:
        cur.execute("INSERT INTO sprocs ( sproc_host_id , sproc_schema , sproc_name ) VALUES ( 1111 , %s, %s ) ", ("apischema","stored_proc_"+str(i)+"(a int, b text)",))
        i += 1

    cur.close()

def create_n_tables(conn,n):
    print "Creating tables..."
    cur = conn.cursor()
    i = 0
    while i < n:
        cur.execute("INSERT INTO tables ( t_host_id , t_name, t_schema ) VALUES ( 1111 , %s, %s ) ", ("table_"+str(i),"dataschema",))
        i += 1

    cur.close()

def create_sproc_data(conn,days,interval):
    print "Creating stored procedure data points..."
    cur = conn.cursor()
    cur.execute("SELECT 'now'::timestamp - ('1 day'::interval * %s)",(days,))
    (n) = cur.fetchone()

    rows = (days*24*60)/(interval)
    print "\tInserting " + str(rows) + " data points for each procedure"

    cur.execute("SELECT sproc_id FROM sprocs WHERE sproc_host_id = 1111")
    sprocs = cur.fetchall()
    j = 1

    for s in sprocs:

        i = 0
        print '\t\tInserting data for sproc_id: '+ str(s[0])

        c = 0
        st = 0
        tt = 0

        while i < rows:

            cur.execute("""INSERT INTO sproc_performance_data(sp_timestamp, sp_sproc_id, sp_calls, sp_self_time, sp_total_time)
                                                    VALUES (%s + (%s * '%s minute'::interval),%s,%s,%s,%s) """, (n,i,interval,s[0],c,st*2000,tt*2000,))

            c += ( i % ( 50 / interval ) ) * 3 + j;
            st += ( i % ( 50 / interval ) ) * 6 + (j*4);
            tt += ( i % ( 50 / interval ) ) * 9 + (j*5);

            i += 1

        j += 1

    cur.close();

def create_load_data(conn,days,interval):
    print "Creating load data points..."
    cur = conn.cursor()
    cur.execute("SELECT 'now'::timestamp - ('1 day'::interval * %s)",(days,))
    n = cur.fetchone()

    rows = (days*24*60)/(interval)
    print "\tInserting " + str(rows) + " data points for Load"

    i = 0
    xlog = 0
    while i < rows:
        l1 = random.randint(300, 2000)
        l5 = random.randint(200, 1500)
        l15 = random.randint(100, 1000)
        xlog += random.randint(100, 1000)
        cur.execute("""INSERT INTO host_load(load_host_id, load_timestamp, load_1min_value, load_5min_value, load_15min_value, xlog_location, xlog_location_mb)
                                     VALUES (%s,%s + (%s * '%s minute'::interval),%s,%s,%s,%s,%s) """,
                                            (1111,n,i,interval,l1,l5,l15,'X/X',xlog))
        i += 1

    cur.close();

def create_table_data(conn,days,interval):
    print "Creating table data points..."
    cur = conn.cursor()

    cur.execute("SELECT 'now'::timestamp - ('1 day'::interval * %s)",(days,))
    n = cur.fetchone()
    cur.execute("select t_id from tables where t_host_id = 1111")
    tables = [ x[0] for x in cur.fetchall() ]

    rows = (days*24*60)/(interval)
    i = 0
    tbl_size = 10**6 * 5
    ind_size = 10**6
    scans = 100
    iscans = 1000
    tupi = 300
    tupu = 200
    tupd = 100
    tupuh = 100
    hread = hhit = iread = ihit = 1000
    while i < rows:
        tbl_size += random.randint(-100000, 200000)
        ind_size += random.randint(-50000, 100000)
        scans = (scans + 2) if random.random() < 0.2 else scans
        iscans = (iscans + 10) if random.random() < 0.2 else iscans
        if random.random() < 0.1:
            tupi += 1000
            tupu += 900
            hread += 100
            iread += 100
        if random.random() < 0.1:
            tupd += random.randint(500,800)
            tupuh += random.randint(400,700)
            hhit += 100
            ihit += 100
        for table in tables:
            cur.execute("""INSERT INTO table_size_data(tsd_table_id, tsd_timestamp, tsd_table_size, tsd_index_size, tsd_seq_scans, tsd_index_scans,
                                                        tsd_tup_ins, tsd_tup_upd, tsd_tup_del, tsd_tup_hot_upd)
                                         VALUES (%s,%s + (%s * '%s minute'::interval),%s,%s,%s,%s,%s,%s,%s,%s) """,
                                                (table,n,i,interval,   tbl_size, ind_size, scans, iscans, tupi, tupu, tupd, tupuh))
            cur.execute("""INSERT INTO table_io_data(tio_table_id, tio_timestamp, tio_heap_read, tio_heap_hit, tio_idx_read, tio_idx_hit)
                                         VALUES (%s,%s + (%s * '%s minute'::interval), %s,%s,%s,%s) """,
                                                (table,n,i,interval,   hread, hhit, iread, ihit))
        i += 1

    cur.close();

def generate_test_data(connection_url, tables, sprocs, days, interval):
    conn = psycopg2.connect(connection_url)
    try:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("SET work_mem TO '64MB';")
        cur.execute("SET search_path TO monitor_data,public;")
        cur.close()

        clear_test_data(conn)

        create_host(conn)

        create_n_sprocs(conn,sprocs)
        create_n_tables(conn,tables)

        create_sproc_data(conn,days,interval)
        create_table_data(conn,days,interval)
        create_load_data(conn,days,interval)
    finally:
        if conn != None:
            conn.close()

DEFAULT_CONF_FILE = '~/.pgobserver.conf'

def main():
    parser = ArgumentParser(description = 'PGObserver testdata generator')
    parser.add_argument('-c', '--config', help = 'Path to config file. (default: %s)' % DEFAULT_CONF_FILE, dest="config" , default = DEFAULT_CONF_FILE)
    parser.add_argument('-gts','--generate-x-tables',help='Number of tables', dest="gt",default=5)
    parser.add_argument('-gps','--generate-x-procs',help='Number of stored procedures', dest="gp",default=12)
    parser.add_argument('-gds','--generate-x-days',help='Number of days', dest="gd",default=5)
    parser.add_argument('-giv','--generate-interval',help='Interval between data in minutes', dest="gi",default=5)

    args = parser.parse_args()

    args.config = os.path.expanduser(DEFAULT_CONF_FILE)

    if not os.path.exists(args.config):
        print 'Configuration file missing:', DEFAULT_CONF_FILE
        parser.print_help()
        return

    with open(args.config, 'rb') as fd:
        settings = json.load(fd)

    connection_url = ' '.join ( ("host="     + settings['database']['host'],
                                 "port="     + str(settings['database']['port']),
                                 "user="     + settings['database']['backend_user'],
                                 "password=" + settings['database']['backend_password'],
                                 "dbname="   + settings['database']['name'] ) )

    print "PGObserver testdata generator:"
    print "=============================="
    print ""
    print "Setting connection string to ... " + connection_url
    print ""
    print "Creating " + str(args.gt) + " tables"
    print "Creating " + str(args.gp) + " stored procedures"
    print "Creating " + str(args.gd) + " days of data"
    print "Creating data points every " + str(args.gi) + " minutes"
    print ""

    generate_test_data(connection_url , args.gt , args.gp,  args.gd,  args.gi)

if __name__ == '__main__':
    main()
