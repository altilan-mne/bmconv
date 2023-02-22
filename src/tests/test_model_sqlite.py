"""Tests of Model module implementation with SQLite database."""

import os, os.path
import sqlite3
import sys
import psutil
import uuid
import time

from datetime import datetime

import exceptions
from model_sqlite import ModelSQLite

class TestModulSQLite:
    """Testing class for SQLite Model implementation"""

    sqlm = ModelSQLite()  # SQLite Model instance

    def _is_conn_open(self, dbname):
        """Internal method to detect if sqlite connection established.
        Should be used with root privileges.

        :param filename of the connected db
        :return True is established otherwise False
        """
        for proc in psutil.process_iter():
            files = proc.open_files()
            if files:
                for _file in files:
                    if _file.path == os.path.abspath(dbname):
                        return True
        return False

    def _dict_factory(self, cursor, row):
        """Row factory returns the dictionary of the SQLite query.

        :param cursor: cursor of the db connection
        :param row: a row of the last query

        :return: the query result as a dictionary in format column_name: column_value
        """
        fields = [column[0] for column in cursor.description]
        return {key: value for key, value in zip(fields, row)}


    def test_init_class(self):
        assert isinstance(self.sqlm, ModelSQLite)
        assert self.sqlm.db_name == ''
        # assert self.sqlm.conn_flag is False
        assert self.sqlm.conn is None
        assert self.sqlm.cur is None
        assert self.sqlm.cwd == os.getcwd()

    def test_create_database(self):
        """This test should be executed under root privileges."""
        # check if filename exists
        filename = 'if_db.sqlite3'
        if not os.path.isfile(filename):
            # create a db file
            f = open(filename, 'w')
            f.close()
        # check exception if new filename already exists
        try:
            self.sqlm.create_database(filename)
        except FileExistsError as e:
            print('\nException FileExistsError raised successfully', e, file=sys.stderr)
        os.remove(filename)  # delete if test file

        # check a new file creating
        filename = 'test_db.sqlite3'  # such a file was deleted previously
        if os.path.isfile(filename):
            os.remove(filename)  # delete test db if it remains from previous tests
        self.sqlm.create_database(filename)
        assert os.path.isfile(filename)  # new file's been created?
        assert self.sqlm.db_name == filename

        # check if connection 'conn' is open to 'test_db.sqlite3'
        assert self._is_conn_open(filename)  # database in open by the connection process, calls from root
        assert self.sqlm.cur.connection == self.sqlm.conn  # if cursor belongs to connection 'conn'

        # check if 3 tables have been created
        store_row_factory = self.sqlm.cur.row_factory  # save current row factory
        self.sqlm.cur.row_factory = sqlite3.Row  # set row factory to generate Row objects
        self.sqlm.cur.execute("SELECT name FROM sqlite_master WHERE type='table'")  # get names of the tables in db
        rows = self.sqlm.cur.fetchall()  # get a list of Row objects
        assert set([row[0] for row in rows]) == {'tree', 'folder', 'url'}  # extract strings and check
        self.sqlm.cur.row_factory = store_row_factory  # restore row factory
        # check if 'roots' record is ok
        store_row_factory = self.sqlm.cur.row_factory  # save current row factory
        self.sqlm.cur.row_factory = self._dict_factory
        self.sqlm.cur.execute("SELECT * "
                              "FROM tree "
                              "INNER JOIN folder ON folder.node_id = tree.guid")
        res_dict = self.sqlm.cur.fetchone()
        assert res_dict['name'] == 'roots'
        assert res_dict['parent_guid'] is None
        assert res_dict['id_no'] == 0
        assert res_dict['node_type'] == 1
        assert res_dict['guid'] == res_dict['node_id']
        assert len(res_dict['guid']) == 36
        assert res_dict['date_added'] == res_dict['date_modified']
        assert len(res_dict['date_added']) == 19

        # check if 'url' table has 0 records
        self.sqlm.cur.execute("SELECT * FROM url")
        assert self.sqlm.cur.fetchone() is None
        self.sqlm.cur.row_factory = store_row_factory  # restore row factory

    def test_add_node(self):
        # minimal folder dict, other fields fill by default
        node_dict = {
            'name': 'folder1', 'parent_name': 'roots'
        }
        node_type = True  # folder
        self.sqlm.add_node(node_dict, node_type)
        # check result
        store_row_factory = self.sqlm.cur.row_factory  # save current row factory
        self.sqlm.cur.row_factory = self._dict_factory
        self.sqlm.cur.execute("SELECT guid "
                              "FROM tree "
                              "WHERE name = 'roots'")
        parent_guid = self.sqlm.cur.fetchone()['guid']  # get 'roots' guid
        self.sqlm.cur.execute("SELECT * "
                              "FROM tree "
                              "INNER JOIN folder ON folder.node_id = tree.guid "
                              "WHERE name = 'folder1'")
        res_dict = self.sqlm.cur.fetchone()  # get the folder record
        assert res_dict['name'] == 'folder1'
        assert res_dict['parent_guid'] == parent_guid
        assert res_dict['id_no'] == 0
        assert res_dict['node_type'] == 1
        assert res_dict['guid'] == res_dict['node_id']
        assert len(res_dict['guid']) == 36
        assert res_dict['date_added'] == res_dict['date_modified']
        assert len(res_dict['date_added']) == 19
        self.sqlm.cur.row_factory = store_row_factory  # restore row factory

        # delete test record folder1
        self.sqlm.cur.execute("DELETE FROM tree "
                              "WHERE name = 'folder1'")
        self.sqlm.conn.commit()

        # maximal folder dict, no fields filled by default
        guid = str(uuid.uuid4())  # get a new guid
        today = datetime.today().replace(microsecond=0)  # get today datetime object
        date_added = datetime.isoformat(today)  # insert the current datetime as a string
        date_modified = date_added
        node_dict = {
            'name': 'folder1', 'parent_name': 'roots',
            'guid': guid,
            'date_added': date_added, 'date_modified': date_modified,
            'id_no': 111
        }
        node_type = True  # folder
        self.sqlm.add_node(node_dict, node_type)
        # check result
        store_row_factory = self.sqlm.cur.row_factory  # save current row factory
        self.sqlm.cur.row_factory = self._dict_factory
        self.sqlm.cur.execute("SELECT * "
                              "FROM tree "
                              "INNER JOIN folder ON folder.node_id = tree.guid "
                              "WHERE name = 'folder1'")
        res_dict = self.sqlm.cur.fetchone()  # get the folder record
        self.sqlm.cur.row_factory = store_row_factory  # restore row factory

        assert res_dict['name'] == 'folder1'
        assert res_dict['parent_guid'] == parent_guid
        assert res_dict['id_no'] == 111
        assert res_dict['node_type'] == 1
        assert res_dict['guid'] == guid
        assert res_dict['date_added'] == date_added
        assert res_dict['date_modified'] == date_modified

        # delete test record folder1
        self.sqlm.cur.execute("DELETE FROM tree "
                              "WHERE name = 'folder1'")
        self.sqlm.conn.commit()

        # minimal url dict, other fields fill by default
        node_dict = {
            'name': 'url1', 'parent_name': 'roots'
        }
        node_type = False  # url
        self.sqlm.add_node(node_dict, node_type)

        # check results
        store_row_factory = self.sqlm.cur.row_factory  # save current row factory
        self.sqlm.cur.row_factory = self._dict_factory

        self.sqlm.cur.execute("SELECT * "
                              "FROM tree "
                              "INNER JOIN url ON url.node_id = tree.guid "
                              "WHERE name = 'url1'")
        res_dict = self.sqlm.cur.fetchone()  # get the folder record
        assert res_dict['name'] == 'url1'
        assert res_dict['parent_guid'] == parent_guid
        assert res_dict['id_no'] == 0
        assert res_dict['node_type'] == 0
        assert res_dict['guid'] == res_dict['node_id']
        assert len(res_dict['guid']) == 36
        assert len(res_dict['date_added']) == 19
        assert res_dict['url'] == ''
        assert res_dict['icon'] == ''
        assert res_dict['keywords'] == ''
        self.sqlm.cur.row_factory = store_row_factory  # restore row factory

        # delete test record url1
        self.sqlm.cur.execute("DELETE FROM tree "
                              "WHERE name = 'url1'")
        self.sqlm.conn.commit()

        # maximal url dict, no fields filled by default
        guid = str(uuid.uuid4())  # get a new guid
        today = datetime.today().replace(microsecond=0)  # get today datetime object
        date_added = datetime.isoformat(today)  # insert the current datetime as a string

        node_dict = {
            'name': 'url1', 'parent_name': 'roots',
            'guid': guid, 'id_no': 222, 'date_added': date_added,
            'url': 'URL', 'icon': 'ICON', 'keywords': 'KEYWORDS'
        }
        node_type = False
        self.sqlm.add_node(node_dict, node_type)  # add a full url

        # check results
        store_row_factory = self.sqlm.cur.row_factory  # save current row factory
        self.sqlm.cur.row_factory = self._dict_factory

        self.sqlm.cur.execute("SELECT * "
                              "FROM tree "
                              "INNER JOIN url ON url.node_id = tree.guid "
                              "WHERE name = 'url1'")
        res_dict = self.sqlm.cur.fetchone()  # get the folder record
        assert res_dict['name'] == 'url1'
        assert res_dict['parent_guid'] == parent_guid
        assert res_dict['id_no'] == 222
        assert res_dict['node_type'] == 0
        assert res_dict['guid'] == res_dict['node_id']
        assert res_dict['guid'] == guid
        assert res_dict['date_added'] == date_added
        assert res_dict['url'] == 'URL'
        assert res_dict['icon'] == 'ICON'
        assert res_dict['keywords'] == 'KEYWORDS'
        self.sqlm.cur.row_factory = store_row_factory  # restore row factory

        # delete test record url1
        self.sqlm.cur.execute("DELETE FROM tree "
                              "WHERE name = 'url1'")
        self.sqlm.conn.commit()

    def test_get_children(self):
        # add the folder2 to roots
        node_dict = {
            'name': 'folder2', 'parent_name': 'roots'
        }
        node_type = True  # folder
        self.sqlm.add_node(node_dict, node_type)
        # add the folder3 to roots
        node_dict = {
            'name': 'folder3', 'parent_name': 'roots'
        }
        node_type = True  # folder
        self.sqlm.add_node(node_dict, node_type)
        # add the url2 to roots
        node_dict = {
            'name': 'url2', 'parent_name': 'roots',
            'url': 'URL2', 'icon': 'ICON2', 'keywords': 'KEYWORDS2'
        }
        node_type = False  # url
        self.sqlm.add_node(node_dict, node_type)

        # get children of roots and check
        res, date = self.sqlm.get_children('roots')
        assert res
        assert date == ('folder2', 'folder3', 'url2')

        # get children of url node 'url2'
        res, date = self.sqlm.get_children('url2')
        assert not res
        assert date == ()

        # check NodeNotExists exception for unknown name of node
        try:
            self.sqlm.get_children('unknown')
        except exceptions.NodeNotExists as e:
            print('\nException NodeNotExists raised successfully', e, file=sys.stderr)

        # delete all child records of 'roots'
        self.sqlm.cur.execute("SELECT guid "
                              "FROM tree "
                              "WHERE name = 'roots'")
        parent_guid = self.sqlm.cur.fetchone()
        self.sqlm.cur.execute("DELETE FROM tree "
                              "WHERE parent_guid = ?", parent_guid)
        self.sqlm.conn.commit()

    def test_delete_node(self):
        # add the folder4 to roots
        node_dict = {
            'name': 'folder4', 'parent_name': 'roots'
        }
        node_type = True  # folder
        self.sqlm.add_node(node_dict, node_type)
        # add the folder5 to roots
        node_dict = {
            'name': 'folder5', 'parent_name': 'roots'
        }
        node_type = True  # folder
        self.sqlm.add_node(node_dict, node_type)
        # add the url3 to roots
        node_dict = {
            'name': 'url3', 'parent_name': 'roots',
            'url': 'URL3', 'icon': 'ICON3', 'keywords': 'KEYWORDS3'
        }
        node_type = False  # url
        self.sqlm.add_node(node_dict, node_type)
        # add the url5_3vto folder5
        node_dict = {
            'name': 'url5_3', 'parent_name': 'folder5',
            'url': 'URL5_3', 'icon': 'ICON5_3', 'keywords': 'KEYWORDS5_3'
        }
        node_type = False  # url
        self.sqlm.add_node(node_dict, node_type)

        # delete url3 from roots
        # get children of roots, check if url3 exists
        res, date = self.sqlm.get_children('roots')
        assert 'url3' in date
        # delete url3 and check if it deleted
        self.sqlm.delete_node('url3')
        res, date = self.sqlm.get_children('roots')
        assert 'url3' not in date

        # delete empty folder4 from roots
        # get children of roots, check if folder4 exists
        res, date = self.sqlm.get_children('roots')
        assert 'folder4' in date
        # delete url3 and check if it deleted
        self.sqlm.delete_node('folder4')
        res, date = self.sqlm.get_children('roots')
        assert 'folder4' not in date

        # try to delete unknown node
        try:
            self.sqlm.delete_node('unknown')
        except exceptions.NodeNotExists as e:
            print('\nException NodeNotExists raised successfully', e, file=sys.stderr)

        # try to delete non-empty folder
        try:
            self.sqlm.delete_node('folder5')
        except exceptions.FolderNotEmpty as e:
            print('\nException FolderNotEmpty raised successfully', e, file=sys.stderr)

        # delete url5_3 from folder5
        # get children of folder5, check if url5_3 exists
        res, date = self.sqlm.get_children('folder5')
        assert 'url5_3' in date
        # delete url3 and check if it deleted
        self.sqlm.delete_node('url5_3')
        res, date = self.sqlm.get_children('folder5')
        assert 'url5_3' not in date

        # delete now empty folder5
        self.sqlm.delete_node('folder5')
        res, date = self.sqlm.get_children('roots')
        assert 'folder5' not in date

    def test_get_node(self):
        # try to get data from an unknown node
        try:
            self.sqlm.get_node('unknown')
        except exceptions.NodeNotExists as e:
            print('\nException NodeNotExists raised successfully', e, file=sys.stderr)

        # add the folder4 to roots
        node_dict = {
            'name': 'folder4', 'parent_name': 'roots'
        }
        node_type = True  # folder
        self.sqlm.add_node(node_dict, node_type)
        # add the folder5 to roots
        node_dict = {
            'name': 'folder5', 'parent_name': 'roots'
        }
        node_type = True  # folder
        self.sqlm.add_node(node_dict, node_type)
        # add the url3 to roots
        node_dict = {
            'name': 'url3', 'parent_name': 'roots',
            'url': 'URL3', 'icon': 'ICON3', 'keywords': 'KEYWORDS3'
        }
        node_type = False  # url
        self.sqlm.add_node(node_dict, node_type)

        res_dict = self.sqlm.get_node('roots')  # get data from roots
        roots_guid = res_dict['guid']
        # check result
        assert len(res_dict) == 7
        assert res_dict['children'] == ['folder4', 'folder5', 'url3']
        assert res_dict['name'] == 'roots'
        assert res_dict['parent_guid'] is None
        assert len(res_dict['guid']) == 36
        assert len(res_dict['date_added']) == 19
        assert res_dict['date_added']  == res_dict['date_modified']
        assert res_dict['id_no'] == 0

        res_dict = self.sqlm.get_node('folder4')  # get data from empty folder 4
        # check result
        assert len(res_dict) == 7
        assert res_dict['children'] == []
        assert res_dict['name'] == 'folder4'
        assert res_dict['parent_guid'] == roots_guid
        assert len(res_dict['guid']) == 36
        assert len(res_dict['date_added']) == 19
        assert res_dict['date_added'] == res_dict['date_modified']
        assert res_dict['id_no'] == 0

        res_dict = self.sqlm.get_node('url3')  # get data from url3
        # check result
        assert len(res_dict) == 8
        assert 'children' not in res_dict.keys()
        assert res_dict['name'] == 'url3'
        assert res_dict['parent_guid'] == roots_guid
        assert len(res_dict['guid']) == 36
        assert len(res_dict['date_added']) == 19
        assert res_dict['id_no'] == 0
        assert res_dict['url'] == 'URL3'
        assert res_dict['icon'] == 'ICON3'
        assert res_dict['keywords'] == 'KEYWORDS3'

        # delete all child records of 'roots'
        self.sqlm.cur.execute("SELECT guid "
                              "FROM tree "
                              "WHERE name = 'roots'")
        parent_guid = self.sqlm.cur.fetchone()
        self.sqlm.cur.execute("DELETE FROM tree "
                              "WHERE parent_guid = ?", parent_guid)
        self.sqlm.conn.commit()

    def test_open_database(self):
        # try to open unknown db
        open_filename = 'unknown_db'
        try:
            self.sqlm.open_database(open_filename)
        except FileNotFoundError as e:
            print('\nException FileNotFoundError raised successfully', e, file=sys.stderr)

        # open db while another connection established
        open_filename = 'test_db.sqlite3'
        self.sqlm.open_database(open_filename)

        assert os.path.isfile(open_filename)  # new file's been created?
        assert self.sqlm.db_name == open_filename

        # check if connection 'conn' is open to 'test_db.sqlite3'
        assert self._is_conn_open(open_filename)  # database in open by the connection process, calls from root
        assert self.sqlm.cur.connection == self.sqlm.conn  # if cursor belongs to connection 'conn'

        # check if 3 tables have been created
        store_row_factory = self.sqlm.cur.row_factory  # save current row factory
        self.sqlm.cur.row_factory = sqlite3.Row  # set row factory to generate Row objects
        self.sqlm.cur.execute("SELECT name FROM sqlite_master WHERE type='table'")  # get names of the tables in db
        rows = self.sqlm.cur.fetchall()  # get a list of Row objects
        assert set([row[0] for row in rows]) == {'tree', 'folder', 'url'}  # extract strings and check
        self.sqlm.cur.row_factory = store_row_factory  # restore row factory
        # check if 'roots' record is ok
        store_row_factory = self.sqlm.cur.row_factory  # save current row factory
        self.sqlm.cur.row_factory = self._dict_factory
        self.sqlm.cur.execute("SELECT * "
                              "FROM tree "
                              "INNER JOIN folder ON folder.node_id = tree.guid")
        res_dict = self.sqlm.cur.fetchone()
        assert res_dict['name'] == 'roots'
        assert res_dict['parent_guid'] is None
        assert res_dict['id_no'] == 0
        assert res_dict['node_type'] == 1
        assert res_dict['guid'] == res_dict['node_id']
        assert len(res_dict['guid']) == 36
        assert res_dict['date_added'] == res_dict['date_modified']
        assert len(res_dict['date_added']) == 19

        # check if 'url' table has 0 records
        self.sqlm.cur.execute("SELECT * FROM url")
        assert self.sqlm.cur.fetchone() is None
        self.sqlm.cur.row_factory = store_row_factory  # restore row factory


        # open db without open connection
        self.sqlm.conn.close()  # close current connection
        self.sqlm.open_database(open_filename)

        assert os.path.isfile(open_filename)  # new file's been created?
        assert self.sqlm.db_name == open_filename

        # check if connection 'conn' is open to 'test_db.sqlite3'
        assert self._is_conn_open(open_filename)  # database in open by the connection process, calls from root
        assert self.sqlm.cur.connection == self.sqlm.conn  # if cursor belongs to connection 'conn'

        # check if 3 tables have been created
        store_row_factory = self.sqlm.cur.row_factory  # save current row factory
        self.sqlm.cur.row_factory = sqlite3.Row  # set row factory to generate Row objects
        self.sqlm.cur.execute("SELECT name FROM sqlite_master WHERE type='table'")  # get names of the tables in db
        rows = self.sqlm.cur.fetchall()  # get a list of Row objects
        assert set([row[0] for row in rows]) == {'tree', 'folder', 'url'}  # extract strings and check
        self.sqlm.cur.row_factory = store_row_factory  # restore row factory
        # check if 'roots' record is ok
        store_row_factory = self.sqlm.cur.row_factory  # save current row factory
        self.sqlm.cur.row_factory = self._dict_factory
        self.sqlm.cur.execute("SELECT * "
                              "FROM tree "
                              "INNER JOIN folder ON folder.node_id = tree.guid")
        res_dict = self.sqlm.cur.fetchone()
        assert res_dict['name'] == 'roots'
        assert res_dict['parent_guid'] is None
        assert res_dict['id_no'] == 0
        assert res_dict['node_type'] == 1
        assert res_dict['guid'] == res_dict['node_id']
        assert len(res_dict['guid']) == 36
        assert res_dict['date_added'] == res_dict['date_modified']
        assert len(res_dict['date_added']) == 19

        # check if 'url' table has 0 records
        self.sqlm.cur.execute("SELECT * FROM url")
        assert self.sqlm.cur.fetchone() is None
        self.sqlm.cur.row_factory = store_row_factory  # restore row factory

    def test_update_node(self):
        # add the folder1 to roots
        node_dict = {
            'name': 'folder1', 'parent_name': 'roots'
        }
        node_type = True  # folder
        self.sqlm.add_node(node_dict, node_type)

        attr_dict = {'name': 'new folder1'}
        self.sqlm.update_node('folder1', attr_dict)
        self.sqlm.cur.execute("SELECT name "
                      "FROM tree "
                      "WHERE name = 'new folder1'")
        assert self.sqlm.cur.fetchone()[0] == 'new folder1'

        # add the url1 to 'new folder1'
        node_dict = {
            'name': 'url1', 'parent_name': 'new folder1',
            'url': 'old url', 'icon': 'old icon', 'keywords': 'old keywords'
        }
        node_type = False  # url
        self.sqlm.add_node(node_dict, node_type)

        # get and store 'date_modified' fields of 'roots' and 'new folder1'
        self.sqlm.cur.execute("SELECT date_modified "
                              "FROM folder")
        dmod = self.sqlm.cur.fetchall()
        roots_date_mod, = dmod[0]  # 'date_modified' of the 'roots'
        folder_date_mod, = dmod[1]  # 'date_modified of the 'new folder1'



        # an empty dict, no updates
        time.sleep(1)  # sleep 1 second
        attr_dict = {}  # no attrs to update
        self.sqlm.update_node('url1', attr_dict)

        store_row_factory = self.sqlm.cur.row_factory  # save current row factory
        self.sqlm.cur.row_factory = self._dict_factory
        self.sqlm.cur.execute("SELECT name, url, icon, keywords "
                              "FROM tree "
                              "INNER JOIN url ON url.node_id = tree.guid "
                              "WHERE name = 'url1'")
        res = self.sqlm.cur.fetchone()
        assert res['name'] == 'url1'
        assert res['url'] == 'old url'
        assert res['icon'] == 'old icon'
        assert res['keywords'] == 'old keywords'
        self.sqlm.cur.row_factory = store_row_factory  # restore row factory
        # check if parent date_modified' did not change
        self.sqlm.cur.execute("SELECT date_added, date_modified "
                              "FROM tree "
                              "INNER JOIN folder ON folder.node_id = tree.guid "
                              "WHERE name = 'new folder1'")
        date_tuple = self.sqlm.cur.fetchone()
        assert date_tuple[0] == date_tuple[1]  # for parent 'new folder1' date_added is equal to date_modified


        # update all but 'keywords' value
        time.sleep(1)  # sleep 1 second
        attr_dict = {'name': 'new url1',
                     'url': 'new url', 'icon': 'new icon'
        }
        self.sqlm.update_node('url1', attr_dict)
        store_row_factory = self.sqlm.cur.row_factory  # save current row factory
        self.sqlm.cur.row_factory = self._dict_factory
        self.sqlm.cur.execute("SELECT name, url, icon, keywords "
                              "FROM tree "
                              "INNER JOIN url ON url.node_id = tree.guid "
                              "WHERE name = 'new url1'")
        res = self.sqlm.cur.fetchone()
        assert res['name'] == 'new url1'
        assert res['url'] == 'new url'
        assert res['icon'] == 'new icon'
        assert res['keywords'] == 'old keywords'
        self.sqlm.cur.row_factory = store_row_factory  # restore row factory
        # check if parent date_modified' changed
        self.sqlm.cur.execute("SELECT date_added, date_modified "
                              "FROM tree "
                              "INNER JOIN folder ON folder.node_id = tree.guid "
                              "WHERE name = 'new folder1'")
        date_tuple = self.sqlm.cur.fetchone()
        assert date_tuple[0] != date_tuple[1]  # for parent 'new folder1' date_added is equal to date_modified

    def test_delete_datebase(self):
        # try to delete unknown db
        delete_filename = 'unknown_db'
        try:
            self.sqlm.delete_database(delete_filename)
        except FileNotFoundError as e:
            print('\nException FileNotFoundError raised successfully', e, file=sys.stderr)

        delete_filename = 'test_db.sqlite3'
        self.sqlm.delete_database(delete_filename)

        assert self.sqlm.conn is None
        assert self.sqlm.db_name == ''
        assert not os.path.isfile(delete_filename)  # new file's been created?




