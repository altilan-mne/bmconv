"""Tests of Model module implementation with internal tree and JSON storage."""

import json
import os, os.path
import sys
from datetime import datetime


import exceptions
from model_json import ModelJSON
from my_nodes import RootBookmarks
from my_nodes import Folder
from my_nodes import Url



class TestModulJSON:
    """Testing class for internal and JSON Model class"""

    jm = ModelJSON()  # JSON Model instance

    def test_init_class(self):
        assert isinstance(self.jm.root, RootBookmarks)
        assert self.jm.root.nodes_dict == {'roots': self.jm.root}
        assert self.jm.tree_name == ''
        assert self.jm.cwd == os.getcwd()

    def test_save_tree(self):
        """Test of saving and deleting database."""
        self.jm.tree_name = 'new_database.json'
        try:
            open(self.jm.tree_name, 'x')  # open a new file as exclusive
        except FileExistsError:
            os.remove(self.jm.tree_name)  # delete this file if it exists
        self.jm._save_tree()
        assert os.path.isfile(self.jm.tree_name)
        os.remove(self.jm.tree_name)  # delete this file if it exists

    def test_create_database(self):
        # check if filename exists
        filename = 'new_db.json'
        if not os.path.isfile(filename):
            # create this file
            f = open(filename, 'w')
            f.close()
        # check exception if input filename already exists
        try:
            self.jm.create_database(filename)
        except FileExistsError as e:
            print('\nException FileExistsError raised successfully', e, file=sys.stderr)
        os.remove(filename)  # delete this file
        # check database creating
        filename = 'new_database.json'
        self.jm.create_database(filename)
        assert os.path.isfile(self.jm.tree_name)
        self.jm.root.__init__()  # clear the root structure
        self.jm.root.nodes_dict['roots'] = self.jm.root  # {'roots': self.root object}  is the first record to the nodes dict
        os.remove(filename)  # delete the file

    def test_add_node(self):
        filename = 'database.json'
        if os.path.isfile(filename):
            os.remove(filename)  # remove the filename if it exists
        self.jm.create_database(filename)  # create an empty db

        # add a folder to the roots
        attr_folder = {'name': 'folder', 'parent_name': 'roots',}
        self.jm.add_node(attr_folder, True)
        # add an url to the previous folder
        attr_url = {'name': 'URL', 'parent_name': 'folder',
                    'url': 'www.url.com', 'icon': 'ICON', 'keywords': 'old keys'}
        self.jm.add_node(attr_url, False)
        # check results
        # folder <roots> first
        node_name = 'roots'
        expected = {'name': 'roots', 'parent_guid': '',
                    'children': ['folder'],}
        expected_nodes_list = ['roots', 'folder', 'URL']
        node_object = self.jm.root.nodes_dict[node_name]  # get the node instance
        node_content = node_object.__dict__.copy()  # local copy of the node's dict
        # ---- replace objects with their names ----
        children_list = [x.name for x in node_content['children']]  # get children names
        node_content['children'] = children_list  # put children names instead of objects
        # assert block
        assert node_content['name'] == expected['name']
        assert node_content['parent_guid'] == expected['parent_guid']
        assert node_content['children'] == expected['children']
        assert type(node_content['guid']) is str
        assert len(node_content['guid']) == 36
        assert type(node_content['date_added']) is str
        assert len(node_content['date_added']) == 19
        assert type(node_content['date_modified']) is str
        assert len(node_content['date_modified']) == 19
        nodes_list = [x for x in node_content['nodes_dict'].keys()]  # names of the global node list
        assert set(nodes_list) == set(expected_nodes_list)

        # folder <folder> second
        parent_guid = node_content['guid']  # keep the guid of parent node from the previous test
        node_name = 'folder'
        expected = {'name': 'folder', 'parent_guid': parent_guid,
                    'children': ['URL'], }
        node_object = self.jm.root.nodes_dict[node_name]  # get the node instance
        node_content = node_object.__dict__.copy()  # local copy of the node's dict
        # ---- replace objects with their names ----
        children_list = [x.name for x in node_content['children']]  # get children names
        node_content['children'] = children_list  # put children names instead of objects
        # assert block
        assert node_content['name'] == expected['name']
        assert node_content['parent_guid'] == expected['parent_guid']
        assert node_content['children'] == expected['children']
        assert type(node_content['guid']) is str
        assert len(node_content['guid']) == 36
        assert type(node_content['date_added']) is str
        assert len(node_content['date_added']) == 19
        assert type(node_content['date_modified']) is str
        assert len(node_content['date_modified']) == 19

        # url <URL> third
        parent_guid = node_content['guid']  # keep the guid of parent node from the previous test
        node_name = 'URL'
        expected = {'name': 'URL', 'parent_guid': parent_guid,
                    'url': 'www.url.com', 'icon': 'ICON', 'keywords': 'old keys', }
        node_object = self.jm.root.nodes_dict[node_name]  # get the node instance
        node_content = node_object.__dict__.copy()  # local copy of the node's dict
        # assert block
        assert node_content['name'] == expected['name']
        assert node_content['parent_guid'] == expected['parent_guid']
        assert type(node_content['guid']) is str
        assert len(node_content['guid']) == 36
        assert type(node_content['date_added']) is str
        assert len(node_content['date_added']) == 19
        assert node_content['url'] == expected['url']
        assert node_content['icon'] == expected['icon']
        assert node_content['keywords'] == expected['keywords']

        self.jm.delete_database(filename)

    def test_get_node(self):
        # wrong node name
        node_name = 'not exist'
        try:
            self.jm.get_node(node_name)
        except exceptions.NodeNotExists as e:
            print('\nException NodeNotExist raised successfully:', e, file=sys.stderr)

        # main test of function
        filename = 'database.json'
        if os.path.isfile(filename):
            os.remove(filename)  # remove the filename if it exists
        self.jm.create_database(filename)  # create an empty db

        # add a folder to the roots
        attr_folder = {'name': 'folder', 'parent_name': 'roots',}
        self.jm.add_node(attr_folder, True)
        # add an url to the previous folder
        attr_url = {'name': 'URL', 'parent_name': 'folder',
                    'url': 'www.url.com', 'icon': 'ICON', 'keywords': 'old keys'}
        self.jm.add_node(attr_url, False)

        # check results
        node_object = self.jm.root.nodes_dict['roots']  # get the roots node instance
        # folder <folder> first
        parent_guid = node_object.guid  # keep the guid of parent node
        node_name = 'folder'
        expected = {'name': 'folder', 'parent_guid': parent_guid,
                    'children': ['URL'], }
        node_content = self.jm.get_node(node_name)
        assert node_content['name'] == expected['name']
        assert node_content['parent_guid'] == expected['parent_guid']
        assert node_content['children'] == expected['children']
        assert type(node_content['guid']) is str
        assert len(node_content['guid']) == 36
        assert type(node_content['date_added']) is str
        assert len(node_content['date_added']) == 19
        assert type(node_content['date_modified']) is str
        assert len(node_content['date_modified']) == 19

        # url <URL> second
        parent_guid = node_content['guid']  # keep the guid of parent node from the previous test
        node_name = 'URL'
        expected = {'name': 'URL', 'parent_guid': parent_guid,
                    'url': 'www.url.com', 'icon': 'ICON', 'keywords': 'old keys', }
        node_content = self.jm.get_node(node_name)
        assert node_content['name'] == expected['name']
        assert node_content['parent_guid'] == expected['parent_guid']
        assert type(node_content['guid']) is str
        assert len(node_content['guid']) == 36
        assert type(node_content['date_added']) is str
        assert len(node_content['date_added']) == 19
        assert node_content['url'] == expected['url']
        assert node_content['icon'] == expected['icon']
        assert node_content['keywords'] == expected['keywords']

        self.jm.delete_database(filename)

    def test_update_node(self):
        # main test of function
        filename = 'database.json'
        if os.path.isfile(filename):
            os.remove(filename)  # remove the filename if it exists
        self.jm.create_database(filename)  # create an empty db

        # add a folder to the roots
        attr_folder = {'name': 'folder', 'parent_name': 'roots', }
        self.jm.add_node(attr_folder, True)
        # add an url to the previous folder
        attr_url = {'name': 'URL', 'parent_name': 'folder',
                    'url': 'www.url.com', 'icon': 'ICON', 'keywords': 'old keys'}
        self.jm.add_node(attr_url, False)

        node_object = self.jm.root.nodes_dict['roots']  # get the roots node instance
        parent_guid = node_object.guid  # keep the guid of parent node

        # update folder
        node_name = 'folder'
        new_attr_folder = {'name': 'FOLDER'}
        self.jm.update_node(node_name, new_attr_folder)  # update folder
        expected = {'name': 'FOLDER', 'parent_guid': parent_guid,
                    'children': ['URL'], }
        node_name = 'FOLDER'
        node_object = self.jm.root.nodes_dict[node_name]  # get the node instance
        node_content = node_object.__dict__.copy()  # local copy of the node's dict
        # ---- replace objects with their names ----
        children_list = [x.name for x in node_content['children']]  # get children names
        node_content['children'] = children_list  # put children names instead of objects
        # get parent date_modified datastamp
        # assert block
        assert node_content['name'] == expected['name']
        assert node_content['parent_guid'] == expected['parent_guid']
        assert node_content['children'] == expected['children']
        assert type(node_content['guid']) is str
        assert len(node_content['guid']) == 36
        assert type(node_content['date_added']) is str
        assert len(node_content['date_added']) == 19
        assert type(node_content['date_modified']) is str
        assert len(node_content['date_modified']) == 19

        node_object = self.jm.root.nodes_dict['FOLDER']  # get the roots node instance
        parent_guid = node_object.guid  # keep the guid of parent node

        # update url
        node_name = 'URL'
        new_attr_url = {'name': 'new_URL', 'url': 'www.google.com', 'icon': 'new_ICON', 'keywords': 'new keys'}
        # get <today> datestamp for following test
        today = datetime.today().replace(microsecond=0)  # get today datetime object
        self.jm.update_node(node_name, new_attr_url)  # update url
        node_name = 'new_URL'
        expected = {'name': 'new_URL', 'parent_guid': parent_guid,
                    'url': 'www.google.com', 'icon': 'new_ICON', 'keywords': 'new keys', }
        node_object = self.jm.root.nodes_dict[node_name]  # get the node instance
        node_content = node_object.__dict__.copy()  # local copy of the node's dict
        # get folder.date_modified stamp
        node_object = self.jm.root.nodes_dict['FOLDER']  # get the roots node instance
        date_modified = node_object.date_modified  # get date_modified field of parent node
        # assert block
        assert node_content['name'] == expected['name']
        assert node_content['parent_guid'] == expected['parent_guid']
        assert type(node_content['guid']) is str
        assert len(node_content['guid']) == 36
        assert type(node_content['date_added']) is str
        assert len(node_content['date_added']) == 19
        assert node_content['url'] == expected['url']
        assert node_content['icon'] == expected['icon']
        assert node_content['keywords'] == expected['keywords']
        assert date_modified == datetime.isoformat(today)

        self.jm.delete_database(filename)

    def test_delete_node(self):
        # create the test database
        filename = 'database.json'
        if os.path.isfile(filename):
            os.remove(filename)  # remove the filename if it exists
        self.jm.create_database(filename)  # create an empty db

        # add a folder to the roots
        attr_folder = {'name': 'folder', 'parent_name': 'roots', }
        self.jm.add_node(attr_folder, True)
        # add an url to the previous folder
        attr_url = {'name': 'URL', 'parent_name': 'folder',
                    'url': 'www.url.com', 'icon': 'ICON', 'keywords': 'old keys'}
        self.jm.add_node(attr_url, False)

        # wrong node name
        node_name = 'not exist'
        try:
            self.jm.delete_node(node_name)
        except exceptions.NodeNotExists as e:
            print('\nException NodeNotExist raised successfully:', e, file=sys.stderr)

        # folder is not empty
        node_name = 'folder'
        try:
            self.jm.delete_node(node_name)
        except exceptions.FolderNotEmpty as e:
            print('\nException FolderNotEmpty raised successfully:', e, file=sys.stderr)

        # delete node URL
        node_name = 'URL'
        self.jm.delete_node(node_name)  # delete node

        # check if URL was deleted from the global node dict
        expected_nodes_list = ['roots', 'folder']  # URL should be deleted
        node_object = self.jm.root.nodes_dict['roots']  # get the roots node instance
        node_content = node_object.__dict__.copy()  # local copy of the node's dict
        nodes_list = [x for x in node_content['nodes_dict'].keys()]  # names of the global node list
        assert set(nodes_list) == set(expected_nodes_list)

        # check if URL was deleted from the parent children list
        node_object = self.jm.root.nodes_dict['folder']  # get the folder node instance
        node_content = node_object.__dict__.copy()  # local copy of the node's dict
        assert node_content['children'] == []  # parent children list is empty

        # then delete now empty folder
        node_name = 'folder'
        self.jm.delete_node(node_name)

        # check if folder was also deleted
        expected_nodes_list = ['roots']  # folder should be deleted
        node_object = self.jm.root.nodes_dict['roots']  # get the roots node instance
        node_content = node_object.__dict__.copy()  # local copy of the node's dict
        nodes_list = [x for x in node_content['nodes_dict'].keys()]  # names of the global node list
        assert set(nodes_list) == set(expected_nodes_list)

    def test_delete_database(self):
        # if deleting filename does not exist
        filename = 'no_exist'
        try:
            self.jm.delete_database(filename)
        except FileNotFoundError as e:
            print('\nException FileNotFoundError raised successfully:', e, file=sys.stderr)

        # test body
        # create and fill a database
        filename = 'database.json'
        if os.path.isfile(filename):
            os.remove(filename)  # remove the filename if it exists
        self.jm.create_database(filename)  # create an empty db

        # add a folder to the roots
        attr_folder = {'name': 'folder', 'parent_name': 'roots', }
        self.jm.add_node(attr_folder, True)
        # add an url to the previous folder
        attr_url = {'name': 'URL', 'parent_name': 'folder',
                    'url': 'www.url.com', 'icon': 'ICON', 'keywords': 'old keys'}
        self.jm.add_node(attr_url, False)
        #
        self.jm.delete_database(filename)  # delete this file

        # assert block
        assert not os.path.isfile(filename)  # if the file has been deleted
        assert isinstance(self.jm.root.nodes_dict['roots'], RootBookmarks)  # if global dict has been cleared
        assert self.jm.root.children == []  # empty children list of the roots node

    def test_open_database(self):
        # if opening filename does not exist
        filename = 'no_exist'
        try:
            self.jm.delete_database(filename)
        except FileNotFoundError as e:
            print('\nException FileNotFoundError raised successfully:', e, file=sys.stderr)
        # test body
        # create and fill a database
        filename = 'database.json'
        if os.path.isfile(filename):
            os.remove(filename)  # remove the filename if it exists
        self.jm.create_database(filename)  # create an empty db

        # add a folder to the roots
        attr_folder = {'name': 'folder', 'parent_name': 'roots', }
        self.jm.add_node(attr_folder, True)
        # add an url to the previous folder
        attr_url = {'name': 'URL', 'parent_name': 'folder',
                    'url': 'www.url.com', 'icon': 'ICON', 'keywords': 'old keys'}
        self.jm.add_node(attr_url, False)

        # clear internal structure, database file holds on the disk
        self.jm.root.__init__()  # clear the root structure
        self.jm.root.nodes_dict['roots'] = self.jm.root  # {'roots': self.root object}  is the first record to the nodes dict

        self.jm.open_database(filename)  # open the previous database

        # check results
        # folder <roots> first
        node_name = 'roots'
        expected = {'name': 'roots', 'parent_guid': '',
                    'children': ['folder'],}
        expected_nodes_list = ['roots', 'folder', 'URL']
        node_object = self.jm.root.nodes_dict[node_name]  # get the node instance
        node_content = node_object.__dict__.copy()  # local copy of the node's dict
        # ---- replace objects with their names ----
        children_list = [x.name for x in node_content['children']]  # get children names
        node_content['children'] = children_list  # put children names instead of objects
        # assert block
        assert node_content['name'] == expected['name']
        assert node_content['parent_guid'] == expected['parent_guid']
        assert node_content['children'] == expected['children']
        assert type(node_content['guid']) is str
        assert len(node_content['guid']) == 36
        assert type(node_content['date_added']) is str
        assert len(node_content['date_added']) == 19
        assert type(node_content['date_modified']) is str
        assert len(node_content['date_modified']) == 19
        nodes_list = [x for x in node_content['nodes_dict'].keys()]  # names of the global node list
        assert set(nodes_list) == set(expected_nodes_list)

        # folder <folder> second
        parent_guid = node_content['guid']  # keep the guid of parent node from the previous test
        node_name = 'folder'
        expected = {'name': 'folder', 'parent_guid': parent_guid,
                    'children': ['URL'], }
        node_object = self.jm.root.nodes_dict[node_name]  # get the node instance
        node_content = node_object.__dict__.copy()  # local copy of the node's dict
        # ---- replace objects with their names ----
        children_list = [x.name for x in node_content['children']]  # get children names
        node_content['children'] = children_list  # put children names instead of objects
        # assert block
        assert node_content['name'] == expected['name']
        assert node_content['parent_guid'] == expected['parent_guid']
        assert node_content['children'] == expected['children']
        assert type(node_content['guid']) is str
        assert len(node_content['guid']) == 36
        assert type(node_content['date_added']) is str
        assert len(node_content['date_added']) == 19
        assert type(node_content['date_modified']) is str
        assert len(node_content['date_modified']) == 19

        # url <URL> third
        parent_guid = node_content['guid']  # keep the guid of parent node from the previous test
        node_name = 'URL'
        expected = {'name': 'URL', 'parent_guid': parent_guid,
                    'url': 'www.url.com', 'icon': 'ICON', 'keywords': 'old keys', }
        node_object = self.jm.root.nodes_dict[node_name]  # get the node instance
        node_content = node_object.__dict__.copy()  # local copy of the node's dict
        # assert block
        assert node_content['name'] == expected['name']
        assert node_content['parent_guid'] == expected['parent_guid']
        assert type(node_content['guid']) is str
        assert len(node_content['guid']) == 36
        assert type(node_content['date_added']) is str
        assert len(node_content['date_added']) == 19
        assert node_content['url'] == expected['url']
        assert node_content['icon'] == expected['icon']
        assert node_content['keywords'] == expected['keywords']

        self.jm.delete_database(filename)

    def test_get_children(self):
        filename = 'database.json'
        if os.path.isfile(filename):
            os.remove(filename)  # remove the filename if it exists
        self.jm.create_database(filename)  # create an empty db

        # add a folder to the roots
        attr_folder = {'name': 'folder', 'parent_name': 'roots',}
        self.jm.add_node(attr_folder, True)
        # add an 3 urls to the previous folder
        attr_url = {'name': 'URL_1', 'parent_name': 'folder',
                    'url': 'www.url.com', 'icon': 'ICON', 'keywords': 'old keys'}
        self.jm.add_node(attr_url, False)
        attr_url = {'name': 'URL_2', 'parent_name': 'folder',
                    'url': 'www.url.com', 'icon': 'ICON', 'keywords': 'old keys'}
        self.jm.add_node(attr_url, False)
        attr_url = {'name': 'URL_3', 'parent_name': 'folder',
                    'url': 'www.url.com', 'icon': 'ICON', 'keywords': 'old keys'}
        self.jm.add_node(attr_url, False)

        # check results
        # if node_name does not exist
        node_name = 'not exist'
        try:
            self.jm.delete_node(node_name)
        except exceptions.NodeNotExists as e:
            print('\nException NodeNotExist raised successfully:', e, file=sys.stderr)

        node_name = 'folder'  # parent folder name has 3 urls
        result, data = self.jm.get_children(node_name)
        assert result  # True for a folder
        assert data == ('URL_1', 'URL_2', 'URL_3')

        node_name = 'URL_1'  # an url does not have children
        result, data = self.jm.get_children(node_name)
        assert not result  # False for a folder
        assert data == ()  # empty tuple

        self.jm.delete_database(filename)  # delete the test database
