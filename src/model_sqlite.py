"""A Model part of the Bookmark manager, implemented with SQLLite database.
Database connection is kept open during the application works as SQLite is a local db.
SQLite row_factor by default is None, return a tuple.
"""

import sqlite3
import uuid
from datetime import datetime
import os
import typing as t

import exceptions

class ModelSQLite:
    """Class of the Model part for Bookmarks manager.
    A SQLite implementation.

    """

    def __init__(self):
        """Constructor method"""
        self.db_name = ''  # name of the connected database, empty by default
        self.conn: sqlite3.Connection = None  # type: ignore[assignment]
        self.cur: sqlite3.Cursor = None  # type: ignore[assignment]
        self.cwd = os.getcwd()  # current working directory

    def _dict_factory(self, cursor: sqlite3.Cursor, row: tuple[t.Any, ...]):
        """Row factory returns the dictionary of the SQLite query.

        :param cursor: cursor of the db connection
        :param row: a row of the last query

        :return: the query result as a dictionary in format column_name: column_value
        """
        fields = [column[0] for column in cursor.description] # get column names as a list of keys
        return {key: value for key, value in zip(fields, row)}  # return result dictionary

    def get_children(self, node_name: str) -> tuple[bool, tuple[str, ...]]:
        """Get a list of child names of the node.

        :exceptions: NodeNotExists if node_name does not exist

        :param node_name: name of a node
        :return: True/False, tuple of child's names/empty tuple
        """
        self.cur.execute("SELECT node_type FROM tree "
                         "WHERE name = ?", (node_name, ))  # the query of the requesting node
        res = self.cur.fetchone()  # unpack the tuple here is not possible in None case
        if res is None:
            # node does not exist
            raise exceptions.NodeNotExists(node_name)  # raise error exception
        # check if this node is a folder
        if res[0]:  # res is unpacked tuple, node_type has the index 0
            # this is a folder
            # get guid of 'node_name'
            self.cur.execute("SELECT guid "
                             "FROM tree "
                             "WHERE name = ?", (node_name, ))
            guid = self.cur.fetchone()  # guid
            # get the children list
            self.cur.execute("SELECT name "
                             "FROM tree "
                             "WHERE parent_guid = ?", guid)
            children = tuple([row[0] for row in self.cur.fetchall()])
            return True, children  # return True, tuple of children
        else:
            # it is an url, no children
            return False, ()  # return False, empty tuple for url

    def add_node(self, attr_dict: dict, node_type: bool):
        """Add a folder or url to the SQLite tree.
        DB connection and cursor have established by db create or open methods.
        Common  dict fields (in the 'tree' table order) -
            guid  - to fill if empty or omitted, primary key
            parent_guid - to find from parent_name field and fill,
            id_no - set 0 if it's omitted otherwise copy,
            name - no changes, simply copy,
            date_added - to fill if empty or omitted

            node_type BOOL - from node_type param,

        Folder dict fields (in the 'folder' table order) -
            node_id - fill from 'guid' of 'tree' table, primary and foreign key
            date_modified to fill if empty or omitted

        Url dict fields (in the 'url' table order) -
            node_id - fill from 'guid' of 'tree' table, primary and foreign key
            url - set empty if it is omitted otherwise copy
            icon - set empty if it is omitted otherwise copy
            keywords - set empty if it is omitted otherwise copy

        :param attr_dict: dictionary with initial node attributes
        :param node_type: True for folder adding, False for url
        :return: nothing
        """
        # prepare dict for INSERT INTO tree
        tree_dict: dict[str, t.Union[str, int, bool]] = {}  # not should be ordered, INSERT by named placeholders

        # check if guid is omitted or empty
        if 'guid' not in attr_dict or not attr_dict['guid']:
            tree_dict['guid'] = str(uuid.uuid4())  # get new guid
        else:
            tree_dict['guid'] = attr_dict['guid']  # copy from incoming guid

        # find and fill parent guid from parent name
        self.cur.execute("SELECT guid "
                         "FROM tree "
                         "WHERE name = ?", (attr_dict['parent_name'], ))
        tree_dict['parent_guid'], = self.cur.fetchone()

        # copy id_no or by default
        if 'id_no' in attr_dict:
            tree_dict['id_no'] = attr_dict['id_no']
        else:
            tree_dict['id_no'] = 0  # by default

        # copy name
        tree_dict['name'] = attr_dict['name']

        # check if date_added is omitted or empty
        if 'date_added' not in attr_dict or not attr_dict['date_added']:
            today = datetime.today().replace(microsecond=0)  # get today datetime object
            tree_dict['date_added'] = datetime.isoformat(today)  # insert the current datetime as a string
        else:
            tree_dict['date_added'] = attr_dict['date_added']  # copy incoming value

        # add node_type into dict because it disallows combining named and qmarks placeholders methods
        tree_dict['node_type'] = node_type

        # insert a record into the tree table
        self.cur.execute("INSERT INTO tree "
                         "(guid, parent_guid, id_no, name, date_added, node_type) "
                         "VALUES (:guid, :parent_guid, :id_no, :name, :date_added, :node_type)",
                         tree_dict)

        # check node_type and fill the child table
        if node_type:
            # this is a folder
            folder_dict = {'node_id': tree_dict['guid']}  # create a dict for a query
            # check if date_modified is omitted or empty
            if 'date_modified' not in attr_dict or not attr_dict['date_modified']:
                folder_dict['date_modified'] = tree_dict['date_added']  # set new date_modifies
            else:
                folder_dict['date_modified'] = attr_dict['date_modified']  # simply copy

            # insert linked record into the folder table
            self.cur.execute("INSERT INTO folder "
                             "(node_id, date_modified) "
                             "VALUES (:node_id, :date_modified)",
                             folder_dict)
        else:
            # this is an url
            url_dict: dict[str, t.Union[str, int, bool]] = {'url': '', 'icon': '', 'keywords': '',
                                                            'node_id': tree_dict['guid']}  # default values
            # copy attrs if they were given or rest them by default
            if 'url' in attr_dict:
                url_dict['url'] = attr_dict['url']  # copy if given
            if 'icon' in attr_dict:
                url_dict['icon'] = attr_dict['icon']  # copy if given
            if 'keywords' in attr_dict:
                url_dict['keywords'] = attr_dict['keywords']  # copy if given

            # insert linked record to the url table
            self.cur.execute("INSERT INTO url "
                             "(node_id, url, icon, keywords) "
                             "VALUES (:node_id, :url, :icon, :keywords)",
                             url_dict)

        self.conn.commit()  # close the transaction

    def update_node(self, name: str, attr_dict: dict):
        """Update a folder or url of the SQLite database.
        User can update:
            name - for folder and url nodes
            url, icon, keywords - for url nodes
            date_modified - will be updated automatically for the parent folder of the updating node

        :param name: updating node name
        :param attr_dict: dictionary with the updating fields
        :return: nothing
        """
        if not attr_dict:
            # no attrs, no actions
            return
        # multi table update does not support by sqlite
        # so modify 2 tables in one transaction
        new_name = name  # preserve if name field will not be updated
        if 'name' in attr_dict:
            # update name field of folder or url at first if name field was given
            self.cur.execute("UPDATE tree "
                             "SET name = ? "
                             "WHERE name = ?", (attr_dict['name'], name))
            new_name = attr_dict.pop('name')  # store the new node name and delete it from attrs dict
        if attr_dict:
            # non-empty, without field 'name'
            # set dict row factory
            old_row_factory = self.cur.row_factory  # save current row factory
            self.cur.row_factory = self._dict_factory  # type: ignore[assignment]

            # to update url attrs get old values at first
            self.cur.execute("SELECT url, icon, keywords, node_id "
                             "FROM tree "
                             "INNER JOIN url ON url.node_id = tree.guid "
                             "WHERE name = ?", (new_name, ))
            stored_attrs = self.cur.fetchone()  # existing attrs
            self.cur.row_factory = old_row_factory  # restore row factory

            updated_attrs = stored_attrs | attr_dict  # merge existing and incoming dicts, incoming has a priority
            # update url attrs with new (or old) values
            self.cur.execute("UPDATE url "
                             "SET (url, icon, keywords) = (:url, :icon, :keywords) "
                             "WHERE node_id = :node_id", updated_attrs)
        # change 'date_modified' field of the parent folder
        self.cur.execute("SELECT parent_guid "
                         "FROM tree "
                         "WHERE name = ?", (new_name,))
        parent_guid = self.cur.fetchone()[0]  # get guid of the parent folder of the new_name
        today = datetime.today().replace(microsecond=0)  # get today datetime object
        date_modified = datetime.isoformat(today)  # get the current datetime as a string
        self.cur.execute("UPDATE folder "
                         "SET date_modified = ? "
                         "WHERE node_id = ?",
                         (date_modified, parent_guid))
        self.conn.commit()  # close the transaction

    def delete_node(self, name: str):
        """Delete a node from the sqlite db.
        Delete the row from 'tree' table.
        Delete the child row automatically with ON DELETE CASCADE option

        :raises NodeNotExists: if node_name does not exist
        :raises FolderNotEmpty: if node_name folder is not empty

        :param name: node name to delete
        :return: nothing
        """
        # check if a such record exists
        # if node 'name' does not exist raise NodeNotExists from get_children()
        res, children = self.get_children(name)  # get the type of node and children
        # if we are here, so the name exists
        # check is it a non-empty folder
        if res and children:
            raise exceptions.FolderNotEmpty(name)  # error

        # empty folder or url, delete it
        # the child record from folder or url tables will be deleted automatically
        # by ON DELETE CASCADE options, see create_database()
        self.cur.execute("DELETE FROM tree "
                         "WHERE name = ?", (name, ))
        self.conn.commit()

    def get_node(self, name: str) -> dict[str, t.Any]:
        """Get a node content as a dict.
        For folder node return a list of children names as 'children': [str, ...] key, value pair
        Does not duplicate primary keys 'node_id' of 'folder' and 'url' child tables
        Delete node_type column from resulting dictionary

        :exceptions: raise NodeNotExists if node name does not exist

        :param name: node name
        :return: dictionary {field_name: field_value} of the node
        """
        # check if record 'name' exists
        # if node 'name' does not exist raise NodeNotExists from get_children()
        res, children = self.get_children(name)  # get the type of node and children
        # if we are here, so the name exists
        old_row_factory = self.cur.row_factory  # save current row factory
        self.cur.row_factory = self._dict_factory  # type: ignore[assignment]
        if res:
            # this is a folder, get data from it
            self.cur.execute("SELECT * "
                             "FROM tree "
                             "INNER JOIN folder ON folder.node_id = tree.guid "
                             "WHERE name = ?", (name, ))  # query from tree and folder tables
            res_dict = self.cur.fetchone()  # get the dictionary
            res_dict['children'] = list(children)  # add children list
        else:
            # this is an url, get data from it
            self.cur.execute("SELECT * "
                             "FROM tree "
                             "INNER JOIN url ON url.node_id = tree.guid "
                             "WHERE name = ?", (name, ))  # query from tree and url tables
            res_dict = self.cur.fetchone()  # get the dictionary

        self.cur.row_factory = old_row_factory  # restore row factory
        del res_dict['node_id']  # delete duplicated primary key of the child table
        del res_dict['node_type']  # delete unexpected field node_type
        return res_dict



    # ---- database section ----
    def create_database(self, name: str):
        """Create a database file and empty SQL bookmark structure.
        Add 'roots' node of the tree.
        Close existing db connection if someone is opened.

        :exceptions: FileExistsError if given filename exists

        :param name: name of the new database, filename for it
        :return: nothing
        """
        if os.path.isfile(name):  # does the file already exist?
            raise FileExistsError(name)  # yes, exception raises

        if self.conn is not None:
            # connection established, close it before
            self.conn.close()
            self.db_name = ''  # reset the name of the current db

        # ---- create a new db file, connection and cursor ----
        self.db_name = name  # store the name of current db
        self.conn = sqlite3.connect(name)  # create a file and connect to it
        self.conn.execute("PRAGMA FOREIGN_KEYS = ON")  # statement for switch on the 'ON DELETE CASCADE' option
        self.cur = self.conn.cursor()  # create a db cursor

        # ---- create 3 tables: tree, folder, url ----
        self.cur.execute("""
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
        """)  # create the main table 'tree'

        self.cur.execute("""
                    CREATE TABLE folder
                        ( 
                        node_id CHAR(36) NOT NULL,
                        date_modified CHAR(19) NOT NULL,
                        CONSTRAINT folder_pk PRIMARY KEY (node_id),
                        CONSTRAINT folder_fk FOREIGN KEY (node_id) REFERENCES tree (guid) ON DELETE CASCADE
                        )
                """)  # create the table of folder attrs

        self.cur.execute("""
            CREATE TABLE url
                ( 
                node_id CHAR(36) NOT NULL,
                url TEXT DEFAULT '',
                icon TEXT DEFAULT '',
                keywords TEXT DEFAULT '',
                CONSTRAINT url_pk PRIMARY KEY (node_id),
                CONSTRAINT url_fk FOREIGN KEY (node_id) REFERENCES tree (guid) ON DELETE CASCADE
                )
        """)  # create the table of url attrs

        # ---- create the 'roots' node ----
        roots_guid = str(uuid.uuid4())  # new guid for 'roots' folder
        parents_roots_guid = None  # parent guid for 'roots' is None
        roots_name = 'roots'  # roots name
        today = datetime.today().replace(microsecond=0)  # get today datetime object
        roots_date = datetime.isoformat(today)  # insert the current datetime as a string

        self.cur.execute("""
            INSERT INTO tree (guid, parent_guid, name, date_added, node_type)
            VALUES (?, ?, ?, ?, True)""",
            (roots_guid, parents_roots_guid, roots_name, roots_date)
        )  # set 'roots' for table tree

        self.cur.execute("""
            INSERT INTO folder (node_id, date_modified)
            VALUES (?, ?)""",
            (roots_guid, roots_date)
        )  # set 'roots' folder attrs

        self.conn.commit()  # execute and commit initial transaction

    def open_database(self, name: str):
        """Open an existing SQLite database.
        Close existing db connection if someone is opened.

        :exception: FileNotFoundError if the filename does not exist

        :param name: name and filename of the opening database
        :return: nothing
        """
        if not os.path.isfile(name):  # does the file exist?
            raise FileNotFoundError(name)  # no, exception raises

        if self.conn is not None:
            # connection established, close it before
            self.conn.close()
            self.db_name = ''  # reset the name of the current db

        # ---- open the db file, connection and cursor ----
        self.db_name = name  # store the name of current db
        self.conn = sqlite3.connect(name)  # create a file and connect to it
        self.conn.execute("PRAGMA FOREIGN_KEYS = ON")  # statement for switch on ON DELETE CASCADE option
        self.cur = self.conn.cursor()  # create a db cursor

    def delete_database(self, name):
        """Delete the database file.
        Close connection and reset db_name, connection flag before.

        :exception: FileNotFoundError if the filename does not exist

        :param name: name and filename of the deleting database
        :return: nothing
        """
        if not os.path.isfile(name):  # does the file exist?
            raise FileNotFoundError(name)   # no, exception raises

        self.conn.close()  # close current connection
        self.conn = None  # type: ignore[assignment]#  set flag not_connected
        self.db_name = ''  # clear name of the current db
        os.remove(name)  # remove the db file

    # ---- convertors section ----
    def convert_chrome(self, filename: str) -> tuple[bool, str]:
        """Convert Chrome bookmark JSON filename to the current tree. Return (True/False, error message)

        :param filename: Google bookmark filename to convert
        :return: (True, empty string)  or (False, error message)
        """
        return True, ''
    def convert_mozilla(self, filename: str) -> tuple[bool, str]:
        """Convert Mozilla bookmark filename to the current tree. Return (True/False, error message).

        :param filename: Mozilla bookmark filename to convert
        :return: (True, empty string)  or (False, error message)
        """
        return True, ''


