"""A Model part of the Bookmark manager, implemented with SQLLite database."""

import sqlite3
import uuid
from datetime import datetime
import os


try:
    os.remove('../src/data/bm.db')
except FileNotFoundError:
    pass

conn = sqlite3.connect('../src/data/bm.db')
cur = conn.cursor()

cur.execute("""
    CREATE TABLE tree
        ( 
        guid CHAR(36) NOT NULL,
        parent_guid CHAR(36),
        id_no INT DEFAULT 0,
        name VARCHAR(4096) NOT NULL,
        date_added CHAR(19) NOT NULL,
        node_type BOOL,
        CONSTRAINT node_pk PRIMARY KEY (guid),
        CONSTRAINT node_ak UNIQUE (name)
        )
""")

node_id = 'node_id'
url = 'url'
arg_list = ['node_id', 'url']
cur.execute("""
    CREATE TABLE url
        ( 
        {} CHAR(36) NOT NULL,
        {} TEXT DEFAULT '',
        icon TEXT DEFAULT '',
        keywords TEXT DEFAULT '',
        CONSTRAINT url_pk PRIMARY KEY (node_id),
        CONSTRAINT url_fk FOREIGN KEY (node_id) REFERENCES tree (guid)
        )
""".format(*arg_list))  # or .format(node_id, url)

roots_guid = str(uuid.uuid4())
parents_roots_guid = None
roots_name = 'roots'
today = datetime.today().replace(microsecond=0)    # get today datetime object
roots_date_added = datetime.isoformat(today)  # insert the current datetime as a string

# cur.execute("INSERT INTO tree (guid, parent_guid, name, date_added, node_type) VALUES (?, ?, ?, ?, True)",
#             (roots_guid, parents_roots_guid, roots_name, roots_date_added))

# column_list = ['guid', 'parent_guid', 'name', 'date_added', 'node_type']
# cur.execute("INSERT INTO tree ({}, {}, {}, {}, {}) VALUES (?, ?, ?, ?, True)".format(*column_list),
#             (roots_guid, parents_roots_guid, roots_name, roots_date_added))

column_list = ['guid', 'parent_guid', 'name', 'date_added', 'node_type']
cur.execute("INSERT INTO tree ({}, {}, {}, {}, {}) VALUES (?, ?, ?, ?, True)".format(*column_list),
            (roots_guid, parents_roots_guid, roots_name, roots_date_added))



conn.commit()

parent_name = 'roots'
cur.execute("SELECT guid FROM tree WHERE name='roots'")
parent_guid = cur.fetchone()[0]
print(parent_guid)

guid = str(uuid.uuid4())
name = 'node1'
date_added = datetime.isoformat(today)  # insert the current datetime as a string

cur.execute("INSERT INTO tree (guid, parent_guid, name, date_added, node_type) VALUES (?, ?, ?, ?, True)",
            (guid, parent_guid, name, date_added))

print(conn.in_transaction)

guid = str(uuid.uuid4())
name = 'node2'
date_added = datetime.isoformat(today)  # insert the current datetime as a string

cur.execute("INSERT INTO tree (guid, parent_guid, name, date_added, node_type) VALUES (?, ?, ?, ?, FALSE)",
            (guid, parent_guid, name, date_added))

cur.execute("INSERT INTO url (node_id, url, icon, keywords) VALUES (?, 'url', 'icon', 'keywords')", (guid, ))

conn.commit()
print(conn.in_transaction)
# get children names

cur.execute("SELECT name FROM tree WHERE parent_guid='{}'".format(parent_guid))
children = cur.fetchall()
print(children)

# -------- 2 tables with WHERE ----------------
cur.execute("SELECT tree.name, tree.date_added, url.url, url.icon, url.keywords "
            "FROM tree, url "
            "WHERE url.node_id = tree.guid")
for x in cur.fetchall():
    print(x)
# ---------------------------------------------
# --------- 2 tables with INNER JOIN ----------
# ----- returns only records having pair on both tables, like WHERE
cur.execute("SELECT tree.name, tree.date_added, url.url, url.icon, url.keywords "
            "FROM tree "
            "INNER JOIN url ON url.node_id = tree.guid")
for x in cur.fetchall():
    print(x)
# ---------------------------------------------
# --------- 2 tables with LEFT JOIN ----------
# ------ returns all records of the left table, no matter if they have pair
cur.execute("SELECT tree.name, tree.date_added, url.url, url.icon, url.keywords "
            "FROM tree "
            "LEFT JOIN url ON url.node_id = tree.guid")
for x in cur.fetchall():
    print(x)
# ---------------------------------------------
# --------- 2 tables with CROSS JOIN ----------
# ------ returns all records of the left table, no matter if they have pair
cur.execute("SELECT tree.name, tree.date_added, url.url, url.icon, url.keywords "
            "FROM tree "
            "CROSS JOIN url ON url.node_id = tree.guid")
for x in cur.fetchall():
    print(x)
# ---------------------------------------------
# using row_factory with sqlite3.Row object
cur.row_factory = sqlite3.Row
cur.execute("SELECT 'Earth' AS name, 6377 AS radius")
row = cur.fetchone()
print(type(row))
print(row.keys())
print(row[0])
print(row['name'])
print(row['RADIUS'])

# using row_factory with dict
def dict_factory(cursor, row):
    fields = [column[0] for column in cursor.description]  # 1st item of 7-tuple description is the column name
    return {key: value for key, value in zip(fields, row)}

cur.row_factory = dict_factory
cur.execute("SELECT 'Earth' AS name, 6378 AS radius")
row = cur.fetchone()
print(row)

# using row_factory with namedtuple
from collections import namedtuple

def ntuple_factory(cursor, row):
    nt: namedtuple
    fields = [column[0] for column in cursor.description]
    nt = namedtuple('Row', fields)
    return nt._make(row)

cur.row_factory = ntuple_factory
cur.execute("SELECT 'Earth' AS name, 6377 AS radius")
row = cur.fetchone()
print(row, row.name, row.radius)

# insert into 2 tables in one transaction
print(conn.isolation_level)


# cur.execute("INSERT INTO tree")

# guid = uuid.uuid4()
# print(type(guid))
# guid_int = int(guid)
# print(guid_int)
# print(str(guid))
#
# guid_str = str(guid)
# g = uuid.UUID(guid_str)
# print(int(g))