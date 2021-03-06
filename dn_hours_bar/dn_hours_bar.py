#!/usr/bin/python


import sys,os

import psycopg2
import ConfigParser
import time
import datetime
import re

if __name__ == "__main__":
    os.environ['HOME'] = "/tmp/tmp.FrT3902013"
else:
    if not os.path.exists("/tmp/apache-matplot"):
        os.mkdir("/tmp/apache-matplot")
    os.environ['HOME'] = "/tmp/apache-matplot/"


#from graphtool.graphs.common_graphs import PieGraph
from graphtool.graphs.common_graphs import StackedBarGraph, BarGraph
from graphtool.graphs.graph import TimeGraph


default_table = {
    'starttime':        datetime.datetime.fromtimestamp(time.time()-86400).date(),
    'endtime':          datetime.datetime.fromtimestamp(time.time()).date(),
    'span':             '86400',
    'facility':         '.*',
    'probe':            '.*',
    'vo':               '.*',
    'role':             '.*',
    'user':             '.*',
    'exclude-facility': 'NONE|Generic|Obsolete',
    'exclude-user':     'NONE',
    'includeSuccess':   'true',
    'exclude-vo':       'unknown|other',
    'includeFailed':    'true',
    'exclude-role':     'NONE',
    'title':            'Wallhours by user'
}

class TimeBarGraph( TimeGraph, BarGraph ):
    pass
        
class TimeStackedBarGraph( TimeGraph, StackedBarGraph ):
    pass


def index(**kargs):

    req = kargs['req']
    req.content_type = "image/x-png"
    options = {}
    for key in default_table:
        if kargs.has_key(key):
            if key == 'starttime' or key == 'endtime':
                try:
                    #2011-01-21 23:59:59
                    options[key] = datetime.datetime(*(time.strptime(kargs[key], '%Y-%m-%d %H:%M:%S')[0:6]))
                except Exception, e:
                    raise e
                    options[key] = default_table[key]
            else:
                options[key] = kargs[key]
        else:
            options[key] = default_table[key]
    QueryTG(req, options)

    



def QueryOSG(options):
    pass


def QueryTG(req, options):
    config = ConfigParser.ConfigParser()
    curpath = os.path.abspath(__file__)
    cwd = curpath.rsplit("/", 1)[0]
#    f = open(os.path.join(cwd,"index.html"))
    config.read(os.path.join(cwd,'password.ini'))
    
    server = config.get('teragrid', 'server')
    print server
    port = config.get('teragrid', 'port')
    username = config.get('teragrid','username')
    password = config.get('teragrid', 'password')
    database = config.get('teragrid', 'database')
    connection = psycopg2.connect(host = server, port = port, user = username, password = password, database = database, sslmode='require')
    cur = connection.cursor()

    query = """
    SELECT temptab.EndTime, (p.first_name || ' ' || p.last_name) as dn, sum(temptab.walltime) as walltime \
    FROM \
    ( \
        SELECT TRUNC(EXTRACT(EPOCH FROM end_time::date) / %(span)s) * %(span)s as EndTime, allocation_breakdown_id, resource_id, sum(COALESCE(processors::numeric, nodecount::numeric)*wallduration::numeric)/3600 as walltime \
        FROM acct.jobs j WHERE end_time >= %(starttime)s and end_time < %(endtime)s \
        GROUP by EndTime, allocation_breakdown_id , resource_id \
    ) AS temptab \
    LEFT JOIN acct.allocation_breakdown a ON (temptab.allocation_breakdown_id = a.allocation_breakdown_id) \
    LEFT JOIN acct.people p ON (a.person_id = p.person_id and (p.first_name || ' ' || p.last_name) ~* %(user)s) \
    LEFT JOIN acct.resources r on (r.resource_id = temptab.resource_id and r.resource_code ~* %(facility)s) \
    GROUP BY temptab.EndTime, dn
    """

    open('/tmp/query', 'w').write(cur.mogrify(query, options))

    cur.execute(query, options)
    
    data = cur.fetchone()
    graph_data = {}
    begin_time = int(time.mktime(options['starttime'].timetuple()))
    end_time = int(time.mktime(options['endtime'].timetuple()))
    span = int(options['span'])
    #cn_re = re.compile(".*CN\=([\w|\s|\.|\,]+).*$")
    while data:
        user = data[1]
        if not graph_data.has_key(user):
            graph_data[user] = {}
        #time_occurred = int(time.mktime(data[1].timetuple()) / span) * span
        time_occurred = data[0] #int(time.mktime(data[0].timetuple()))
        graph_data[user][time_occurred] =  data[2]

        data = cur.fetchone()
    #open("/tmp/query", 'w').write(str(graph_data))
    SBG = TimeStackedBarGraph()
    title = 'Wallhours by user'
    if options.has_key('title'):
        title = options['title']
    metadata = {'title':title, 'starttime': begin_time, 'endtime':end_time, 'span': int(options['span'])}
    SBG(graph_data, req, metadata) 

    




def main():
    os.environ['HOME'] = "/tmp/tmp.FrT3902013"
    QueryTG(None, default_table)


if __name__ == "__main__":
    main()
else:
    from mod_python import apache

