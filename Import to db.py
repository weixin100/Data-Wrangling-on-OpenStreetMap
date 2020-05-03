# -*- coding: utf-8 -*-
"""
Created on Sun May  3 09:42:49 2020

@author: wei_x
"""

#To import all .csv file to database "Stanford.db"

import csv, sqlite3

con = sqlite3.connect("Stanford.db")
cur = con.cursor()
cur.execute('DROP TABLE IF EXISTS nodes')

cur.execute('''
	CREATE TABLE nodes (
    id INTEGER PRIMARY KEY NOT NULL,
    lat REAL,
    lon REAL,
    user TEXT,
    uid INTEGER,
    version INTEGER,
    changeset INTEGER,
    timestamp TEXT
)''')
with open('nodes.csv', 'r') as fin:
	dr = csv.DictReader(fin) # comma is the default delimiter
	to_db = [(i['id'], i['lat'], i['lon'], i['user'], i['uid'], i['version'], \
           i['changeset'], i['timestamp'] ) for i in dr]

cur.executemany('INSERT INTO nodes(id, lat, lon, user, uid, version, changeset,\
                                   timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?);', to_db)
# commit the changes
con.commit()
con.close()