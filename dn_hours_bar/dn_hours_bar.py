#!/usr/bin/python


import sys,os

import psycopg2
import ConfigParser

#os.environ['HOME'] = "/tmp/tmp.FrT3902013"
os.environ['HOME'] = "/tmp/apache-matplot/"
from graphtool.graphs.common_graphs import PieGraph


def index(req, user):
    QueryTG({'user': user})
    req.content_type = "image/x-png"
    req.write("")

    



def QueryOSG(options):
    pass


def QueryTG(options):
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
    and dn ~* '%(user)s' \
    order by job_id desc limit 10;" % options )
    
    data = cur.fetchone()
    while data:
        print data
        data = cur.fetchone()


    




def main():
    QueryTG({'user':'.*Derek.*'})


if __name__ == "__main__":
    main()
else:
    from mod_python import apache

