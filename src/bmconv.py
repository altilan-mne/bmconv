"""File convertor for Bookmark manager project.
Convert the format of storage files as JSON, SOLite and more.
Use the command line for the user interface.
Implemented with the built-in argparse library.
Convert all external formats into base internal JSON format.
Converting from external formats to the other internals provides
as cross-formatting via JSON format.

"""
import os
import argparse
from functools import partial
import json
import typing as t
import sqlite3

from my_nodes import RootBookmarks
from my_nodes import Folder
from my_nodes import Url
from time_convert import stamp_to_string


# common data
version = 'Bookmark file converter 2.0'
list_input_formats = ['chrome', 'json', 'sqlite']
list_output_formats = ['json', 'sqlite']

class MyJSONEncoder(json.JSONEncoder):
    """Overwrite the default JSON encoder class from the json module

    """
    def default(self, obj: Folder | Url | RootBookmarks):
        """Customize the encoding of the following custom classes: Root Bookmarks, Folder, Url.

        :param obj: a tree object that is being serialized
        :return: a dictionary of the input object to encode by json.py
        """
        if isinstance(obj, Folder | Url):
            return obj.__dict__  # for serialisation return an object's dict instead of the object
        elif isinstance(obj, RootBookmarks):    # for RootBookmarks a recursive ref to the object has to eliminate
            obj_copy = obj.__dict__.copy()  # make a swallow copy of the tree dict
            del obj_copy['nodes_dict']   # remove the dict of all nodes from json image (for the copy only!!!)
            return obj_copy  # for serialisation return an object's dict instead of the object (edited copy !)
        else:
            super().default(obj)  # the object does not need to be transformed


class JSONToSQLite():
    """A class to convert internal JSON format to the internal SQLite format.

    """

    def __init__(self, filename: str, tree_image: dict):
        """Constructor method.

        :param filename:
        """
        self.cwd = os.getcwd()  # current working directory

        # filename does not exist definitely
        # ---- create a new db file, connection and cursor ----
        self.conn = sqlite3.connect(filename)  # create a file and connect to it
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
        roots_attrs = tree_image.copy()  # a swallow copy of roots field
        roots_attrs['node_type'] = True  # add a node_type value to the params dict
        roots_attrs['node_id'] = roots_attrs['guid']  # add primary key for the folder table
        roots_attrs['parent_guid'] = None  # roots has not the parent guid
        # roots_guid = str(uuid.uuid4())  # new guid for 'roots' folder
        # parents_roots_guid = None  # parent guid for 'roots' is None
        # roots_name = 'roots'  # roots name
        # today = datetime.datetime.today().replace(microsecond=0)  # get today datetime object
        # roots_date = datetime.datetime.isoformat(today)  # insert the current datetime as a string

        self.cur.execute("""
            INSERT INTO tree (guid, parent_guid, name, date_added, node_type)
            VALUES (:guid, :parent_guid, :name, :date_added, :node_type)""",
            roots_attrs)  # set 'roots' for table tree

        self.cur.execute("""
            INSERT INTO folder (node_id, date_modified)
            VALUES (:node_id, :date_modified)""",
            roots_attrs)  # set 'roots' folder attrs

        self.conn.commit()  # execute and commit initial transaction

    def add_node(self, attr_dict: dict, node_type: bool):
        """Add a node to the SQLite database. All node fields already filled.

        :param attr_dict: dictionary with initial node attributes
        :param node_type: True for folder adding, False for url
        :return:
        """
        # add node_type into dict because it disallows combining named and qmarks placeholders methods
        attr_dict['node_type'] = node_type
        # insert a record into the tree table
        self.cur.execute("INSERT INTO tree "
                         "(guid, parent_guid, id_no, name, date_added, node_type) "
                         "VALUES (:guid, :parent_guid, :id_no, :name, :date_added, :node_type)",
                         attr_dict)

        # check node_type and fill the child table
        if node_type:
            # this is a folder
            folder_dict = {'node_id': attr_dict['guid'],
                           'date_modified': attr_dict['date_modified']
                           }  # create a dict for a query

            # insert linked record into the folder table
            self.cur.execute("INSERT INTO folder "
                             "(node_id, date_modified) "
                             "VALUES (:node_id, :date_modified)",
                             folder_dict)
        else:
            # this is an url
            url_dict ={'node_id': attr_dict['guid'],
                       'url': attr_dict['url'],
                       'icon': attr_dict['icon'],
                       'keywords': attr_dict['keywords']
                        }  # default values

            # insert linked record to the url table
            self.cur.execute("INSERT INTO url "
                             "(node_id, url, icon, keywords) "
                             "VALUES (:node_id, :url, :icon, :keywords)",
                             url_dict)
            self.conn.commit()  # close the transaction

    def convert(self, tree_image: t.Any):
        """Convert internal JSON image (copied from file) to the SQLite database.
        Recursive call

        :param tree_image: copy of JSON BM datafile
        :return: (True, empty string)  or (False, error message) and filled tree
        """
        for child in tree_image['children']:  # loop for children list of the folder
            if 'children' in child:  # node has a children list, so it 'is a folder
                self.convert(child)  # recursion call for the nested nodes
                self.add_node(child, True)  # add a folder
            else:
                self.add_node(child, False)  # add an url

class ChromeToTree:
    """A class to convert Chrome bookmark (JSON) format into internal tree format.
    """
    def __init__(self):
        """Constructor method.
        """
        self.root = RootBookmarks()  # create a new bookmark's tree object
        self.root.nodes_dict['roots'] = self.root  # {'roots': self.root object}  is the first record to the nodes dict

    # ---- section of format conversions
    def _chrome_into_object(self, dt: dict) -> dict:
        """Conversion of the Chrome JSON structure to the internal node tree.
        Inner recursion for convert_chrome method, in-place conversion

        :param dt: a dictionary with chrome JSON structure
        :return: intermediate dictionary during recursion execution
        """
        init_len = len(dt['children'])  # an initial length of the input dict
        while init_len:
            attr_dict = dt['children'][0]  # get first child from the list and remove it

            # remove non-using data fields of chrome format
            attr_dict.pop('meta_info', None)  # remove a <meta_info> list if it presents

            # modify source structure to internal
            attr_dict['name'] = self.root.duplicate_name(attr_dict['name'])  # replace duplicate name with name(i)
            attr_dict['id_no'] = attr_dict.pop('id')  # replace key 'id' with 'id_no' keeping its value
            attr_dict['parent_name'] = dt['name']  # set parent name for child object
            if attr_dict.pop('type') == 'folder':
                node_type = True  # this is a folder
                if attr_dict['name'] != 'roots':  # skip format conversion for roots folder
                    # convert timestamps of the folder
                    attr_dict['date_added'] = stamp_to_string(int(attr_dict['date_added']), 'google')
                    attr_dict['date_modified'] = stamp_to_string(int(attr_dict['date_modified']), 'google')
                self.root.add_node(attr_dict, node_type)  # call an appropriated nodes method
                self._chrome_into_object(attr_dict)  # recursion
            else:
                node_type = False  # this is an url
                # convert timestamp of the url
                attr_dict['date_added'] = stamp_to_string(int(attr_dict['date_added']), 'google')
                self.root.add_node(attr_dict, node_type)  # call an appropriated nodes method

            # an object has been created and added
            del dt['children'][0]  # remove a dict of the added node
            init_len -= 1
        return dt  # return the dict where dicts are replaced by equivalent objects - nodes


    def convert(self, filename: str) -> t.Tuple[bool, str]:
        """Convert Chrome bookmark JSON filename to the current tree. Fill RootBookmarks object in-place.

        :param filename: Google bookmark filename to convert
        :return: (True, empty string)  or (False, error message) and filled tree
        """
        # ---- open and load JSON file of Chrome bookmarks, the file definitely exists
        with open(filename, 'r', encoding='utf-8') as f:  # open the tree image file, or raise FileNotFoundError
            source_file = json.load(f)  # read the json image and then close the file, image is a dict

        # ---- extract the roots dict only, checksum, sync_metadata and version attrs do not use
        chrome_keys = list(source_file)  # get chrome main keys of the bookmark object
        if 'checksum' and 'roots' and 'version' not in chrome_keys:  # if keys don't exist that is wrong file format
            return False, f'Source file {filename} has wrong format'  # return with error message

        # ---- this is a chrome bookmark file ----
        # ---- remove unnecessary attrs from source structure: checksum, version, sync_metadata ----
        chrome_roots = source_file['roots']  # roots names and values dictionary only - (name: root), ...
        # ---- prepare a dict to create our internal root from source data
        tree_image = dict()  # clear attrs dict
        tree_image['children'] = list(chrome_roots.values())  # get a root children list, DO NOT VIEW !!!
        self.root.update_root(**tree_image)  # update the current tree with source values

        # ---- decode nested dictionaries from json image to the original objects and add it to the node's list ----
        dt = self.root.__dict__  # start from the root children
        self._chrome_into_object(dt)  # decode nested dictionaries recursively

        return True, ''  # return success


def print_verbose(message: str, verbose):
    """Print message if verbose mode set."""
    if verbose:
        print(message)


def chrome_to_json(source: str, destination: str):
    """Convert Chrome bookmark to internal JSON format.
    Create and fill internal tree then serialise and save data to a file.

    :param source: filename of external Chrome bookmark (JSON format)
    :param destination: filename of BookmarkManager internal JSON format
    :return: a tuple (True, empty string)  or (False, error message)
    """
    conv = ChromeToTree()  # instance of (Chrome -> JSON converter)
    res, error = conv.convert(source)  # convert and get the result
    if res:
        # conversion is ok, try to write the tree into destination file
        try:
            with open(destination, "w") as write_file:
                json.dump(conv.root, write_file, cls=MyJSONEncoder)
        except OSError as e:
            res = False
            error = str(e)
    return res, error  # return result and error if exists


def json_to_sqlite(source: str, destination: str):
    """Convert internal JSON file to SQLite database format.
    Open and read internal JSON file.
    Create SQLite file, open and close connection, check errors.

    :param source: filename of internal bookmarks (JSON format)
    :param destination: filename of BookmarkManager internal SQLite format
    :return: a tuple (True, empty string)  or (False, error message)
    """
    # open and read source datafile
    try:
        # ---- read json database ----
        with open(source, 'r') as f:   # open the tree image file, or FileNotFoundError exception
            tree_image = json.load(f)   # read the json image and then close the file, image is a dict
    except json.JSONDecodeError as e:
        return False, str(e)  # return JSON decode error
    # conversion begins
    try:
        conv = JSONToSQLite(destination, tree_image)  # instance of (JSON -> SQLite converter)
        conv.convert(tree_image)  # convert memory JSON image into SQLite database
        conv.conn.close()  # success, close db connection
        return True, ''
    except sqlite3.Error as e:
        return False, str(e)  # return error message


def main():
    """Main rootine of the file converter program.

    :return: nothing
    """

    ap = argparse.ArgumentParser(
        description='File converter of Bookmark Manager database formats.'
    )
    # default max_help_position of the HelpFormatter class is 24, increase it for nice help output
    new_formatter = partial(argparse.HelpFormatter, max_help_position=28)
    ap.formatter_class = new_formatter

    ap.add_argument('--version', action='version', version=version)
    ap.add_argument('--list_input_formats', action='version', version=', '.join(list_input_formats),
                    help='show list of input formats and exit')
    ap.add_argument('--list_output_formats', action='version', version=', '.join(list_output_formats),
                    help='show list of output formats and exit')
    ap.add_argument('--verbose', action='store_true',
                    help='switch verbose mode ON')
    ap.add_argument('-f', '--from_', choices=list_input_formats,
                    help='source format', required=True)
    ap.add_argument('-t', '--to', choices=list_output_formats,
                    help='destination format', required=True)
    ap.add_argument('source')
    ap.add_argument('destination')
    args = ap.parse_args()

    # check if source exists
    if not os.path.isfile(args.source):
        print_verbose(f"Source file {args.source} does not exist", args.verbose)
        exit(1)

    # check if destination already exist
    if os.path.isfile(args.destination):
        print(f"Destination file {args.destination} already exists.")
        answer = input("Overwrite? [Y/n] --> ")
        if answer.lower() != 'y':
            print_verbose("Conversion canceled.", args.verbose)
            exit(1)
        else:
            os.remove(args.destination)

    # input and output files are ok, print if --verbose
    print_verbose(f"Source: {args.source}", args.verbose)
    print_verbose(f"Destination: {args.destination}", args.verbose)

    # what to do
    match args.from_, args.to:
        case ['chrome', 'json']:  # from Chrome bookmark format to internal JSON
            res, error = chrome_to_json(args.source, args.destination)
        case ['chrome', 'sqlite']:  # from Chrome to internal sqlite
            return
        case ['json', 'sqlite']:  # from internal json to internal sqlite
            res, error = json_to_sqlite(args.source, args.destination)
        case ['sqlite', 'json']:  # from internal sqlite to internal json
            return
        case _:
            print_verbose('Invalid options.', args.verbose)
            exit(1)

    # exit section
    # print if --verbose
    if res:
        print_verbose(f"{args.source} converted to {args.destination} successfully", args.verbose)  # success massage
    else:
        print_verbose(f"Error {error} occurred during conversion.", args.verbose)  # error message
        exit(1)


if __name__ == '__main__':
    main()













