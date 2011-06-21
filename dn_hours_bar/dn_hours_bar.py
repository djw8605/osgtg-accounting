#!/usr/bin/python


import sys,os

import psycopg2
import ConfigParser
import time
import re

#os.environ['HOME'] = "/tmp/tmp.FrT3902013"
os.environ['HOME'] = "/tmp/apache-matplot/"
#from graphtool.graphs.common_graphs import PieGraph
from graphtool.graphs.common_graphs import StackedBarGraph, BarGraph
from graphtool.graphs.graph import TimeGraph


class TimeBarGraph( TimeGraph, BarGraph ):
    pass
        
class TimeStackedBarGraph( TimeGraph, StackedBarGraph ):
    pass


def index(req, user):

    req.content_type = "image/x-png"
    QueryTG(req, {'user': user})

    



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
    cur.execute("select distinct on (job_id) job_id, end_time, dn, wallduration from acct.jobs j, acct.distinguished_names d, acct.allocation_breakdown a \
    where j.allocation_breakdown_id = a.allocation_breakdown_id and a.person_id = d.person_id \
    and dn ~* %(user)s \
    order by job_id desc limit 1000;", options )
    
    data = cur.fetchone()
    graph_data = {}
    begin_time = time.mktime(data[1].timetuple())
    end_time = time.mktime(data[1].timetuple())
    span = 86400
    cn_re = re.compile(".*CN\=([\w|\s|\.|\,]+).*$")
    while data:
        user = cn_re.search(data[2]).group(1)
        if not graph_data.has_key(user):
            graph_data[user] = {}
        time_occurred = int(time.mktime(data[1].timetuple()) / span) * span
        graph_data[user][time_occurred] =  data[3]
        if begin_time > time_occurred:
            begin_time = time_occurred
        if end_time < time_occurred:
            end_time = time_occurred

        data = cur.fetchone()
    open("/tmp/query", 'w').write(str(graph_data))
    SBG = TimeStackedBarGraph()
    metadata = {'title':'Wallhours by user', 'starttime': begin_time, 'endtime':end_time, 'span': 86400}
    SBG(graph_data, req, metadata) 

    




def main():
    QueryTG({'user':'.*Derek.*'})


if __name__ == "__main__":
    main()
else:
    from mod_python import apache

