"""Studying of SQL inject """

import sqlite3
import os

# init db

try:
    os.remove('sql_inject.db')
except FileNotFoundError:
    pass

conn = sqlite3.connect('sql_inject.db')
cur = conn.cursor()

# create the table

cur.execute("CREATE TABLE users ("
            "username VARCHAR(30), "
            "admin BOOLEAN)")

cur.execute("INSERT INTO users (username, admin)"
             "VALUES "
             "('ran', True), "
             "('haki', False)")

conn.commit()

# executing a query
cur.execute("SELECT COUNT(*) FROM users")
res = cur.fetchone()
print(res)

# get admin, BAD EXAMPLE!!!
def is_admin(username: str) -> bool:
    cur.execute("SELECT admin FROM users WHERE username = '%s'" % username)
    res = cur.fetchone()
    if res is None:
        return False
    admin, = res
    return admin

# existing users
print(is_admin('ran'))
print(is_admin('haki'))

# non-existing user
print(is_admin('foo'))  # error as fetchone() returned None, add the None case to is_admin

# SQL inject see below
print(is_admin("' OR True; --"))  # this one query always returning True

# to avoid this use placeholders: qmark or named
# qmark style, variable as tuple is the second arg of execute method

def is_admin_qmark(username: str) -> bool:
    cur.execute("SELECT admin FROM users WHERE username = ?", (username, ))
    res = cur.fetchone()
    if res is None:
        return False
    admin, = res
    return admin


print(is_admin_qmark("' OR True; --"))  # this wrong query returning False with qmark parametrization

# named style of parametrization, second arg is a dict (or a subclass)
# keys are column names of the table, values - row values
# example for multiply rows using executemany(), data as a tuple of dictionaries

data_multi = (
    {"username": 'Bob', 'admin': True},
    {"username": 'Jack', 'admin': False},
    {"username": 'Sue', 'admin': False}
)

cur.executemany("INSERT INTO users (username, admin) "
                "VALUES (:username, :admin)",
                data_multi)
conn.commit()

# ---- END OF STUDYING ----




