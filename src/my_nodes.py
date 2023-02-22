"""The low level module for the bookmark project.
It contains a node's classes and internal database structure

Internal data structure is a tree RootBookmarks.
Root node has the name 'roots' (that everywhere given in literal form).
There are two types of nodes: Folder and Url, node 'roots' is a special form of folder
Folders have a mutable list of children, urls are leaf nodes.
The tree structure keeps a global nodes' dictionary in the form {key=node_name: value=object: Folder | Url}
All nodes have 'guid' and 'parent_guid' fields for reverse tree search

Instances of the class Folder have the following attributes:
    self.guid: str
    self.parent_guid: str
    self.name: str
    self.id_no: int
    self.date_added: str
    self.children: list   # a list of children entities, Folder and Url instances
    self.date_modified: str

Instances of the class Url have the following attributes:
    self.guid
    self.parent_guid
    self.name
    self.id_no
    self.date_added: str
    self.url    # URL of the bookmark
    self.icon   # small graphic icon of the URL
    self.keywords   # the keywords for the URL content
"""

import uuid
import typing as t
from datetime import datetime

import exceptions


class Node(object):
    """The base class of nodes for bookmark's tree.

    """
    def __init__(self, guid: str = '', parent_guid: str = '', name: str = ''):
        """Constructor method.

        :param guid: GUID of a node
        :param parent_guid: GUID of a parent node
        :param name: name of a node
        """
        # set guid, if empty - generate a new guid
        if guid:
            self.guid = guid  # set from input param
        else:
            self.guid = str(uuid.uuid4())  # get a new GUID
        self.parent_guid = parent_guid  # fill at the inserting to the tree
        if name:
            self.name = name  # set from input param
        else:
            self.name = self.guid  # by default

    def update(self, **kwargs):
        """Update params of the Node class.

        :raises raise TypeError if the argument(s) are exceeded

        :param kwargs: guid, parent_guid, name Node fields
        :return: nothing
        """
        if 'guid' in kwargs:
            self.guid = kwargs.pop('guid')
        if 'parent_guid' in kwargs:
            self.parent_guid = kwargs.pop('parent_guid')
        if 'name' in kwargs:
            self.name = kwargs.pop('name')
        if kwargs:
            raise TypeError(f'{len(kwargs)} exceeded argument(s) was/were given')


class Bookmark(Node):
    """Parent class for Folder and Url classes.

    """
    def __init__(self, id_no: int = 0, date_added: str = '', **kwargs):
        """Constructor method.

        :param id_no: identification number, not used now, for Chrome compatibility.
        :param date_added: date when a node was added to the tree
        :param kwargs: other params for superclasses methods
        """
        self.id_no = id_no  # for compatibility with Chrome , will be assigned later
        # set date_added for compatibility with Chrome , might be updated later
        if not date_added:  # date_added parameter is omitted, set it from today datetime
            today = datetime.today().replace(microsecond=0)    # get today datetime object
            self.date_added = datetime.isoformat(today)  # insert the current datetime as a string
        else:
            self.date_added = date_added
        super().__init__(**kwargs)

    def update(self, **kwargs):
        """Update params of the Bookmark class.

        :param kwargs: id_no, date_added Bookmark fields
        :return: nothing
        """
        if 'id_no' in kwargs:
            self.id_no = kwargs.pop('id_no')
        if 'date_added' in kwargs:
            self.date_added = kwargs.pop('date_added')
        super().update(**kwargs)


class Folder(Bookmark):
    """A Folder class, child class of Bookmark class.

    """
    def __init__(self, children: t.Optional[list] = None, date_modified: str = '', **kwargs):
        """Constructor method.

        :param children: list of children, mutable
        :param date_modified: date when a folder was modified
        :param kwargs: other params for superclasses methods
        """
        if children is None:
            self.children = []   # default value of mutable args evaluated at the FIRST CALL only !!!
        else:
            self.children = children  # set from params
        # set date_added for compatibility with Chrome , might be updated later
        if not date_modified:  # date_added parameter is omitted, set it from today datetime
            today = datetime.today().replace(microsecond=0)  # get today datetime object
            self.date_modified = datetime.isoformat(today)  # insert the current datetime as a string
        else:
            self.date_modified = date_modified
        super().__init__(**kwargs)

    def update(self, **kwargs):
        """Update params of the Folder class.

        :param kwargs: children, date_modified Folder fields
        :return: nothing
        """
        if 'children' in kwargs:
            self.children = kwargs.pop('children')
        if 'date_modified' in kwargs:
            self.date_modified = kwargs.pop('date_modified')
        super().update(**kwargs)


class Url(Bookmark):
    """An Url class, child class of Bookmark class.

    """
    def __init__(self, url: str = '', icon:str = '', keywords:str = '', **kwargs):
        """Constructor method.

        :param url: URL address of an internet resource
        :param icon: icon for this website, not used now, for the future development
        :param keywords: keywords for a fast search, not used now, for the future development
        :param kwargs: other params for superclasses methods
        """
        self.url: str = url
        self.icon: str = icon
        self.keywords: str = keywords
        super().__init__(**kwargs)

    def update(self, **kwargs):
        """Update params of the Url class.

        :param kwargs: url, icon, keywords Url fields
        :return: nothing
        """
        if 'url' in kwargs:
            self.url = kwargs.pop('url')
        if 'icon' in kwargs:
            self.icon = kwargs.pop('icon')
        if 'keywords' in kwargs:
            self.keywords = kwargs.pop('keywords')
        super().update(**kwargs)


class RootBookmarks(Node):
    """The root class for bookmark's tree.

    """
    def __init__(self):
        """Constructor method.

        """
        self.nodes_dict: dict = {}  # global dict of all nodes in the tree: {'name': <object>,,,}
        self.children: list = list()  # create the list of child objects
        today = datetime.today().replace(microsecond=0)  # get today datetime object
        self.date_added: str = datetime.isoformat(today)  # insert the current datetime as a string
        self.date_modified: str = datetime.isoformat(today)  # insert the current datetime as a string
        # default values for name and parent_guid, no parent_guid for the root
        super().__init__(name='roots', parent_guid='')

    def update_root(self, **kwargs):
        """Update params of the RootBookmarks class.

        :param kwargs: children, date_added, date_modified RootBookmarks fields
        :return: nothing
        """
        """Here only root children list is changed"""
        if 'children' in kwargs:
            self.children = kwargs.pop('children')
        if 'date_added' in kwargs:
            self.date_added = kwargs.pop('date_added')
        if 'date_modified' in kwargs:
            self.date_modified = kwargs.pop('date_modified')
        super().update(**kwargs)

    def duplicate_name(self, name: str) -> str:
        """Check if a name already exists in the global node dict.
        Replace a duplicate name with a name(i)' where i = 1,2, ... n.

        :param name: node name for checking
        :return: new unique node name or input name if it is not duplicated
        """
        i = 1  # initial copy name number
        new_name = name  # an input name for search
        while new_name in self.nodes_dict:
            new_name = f'{name} ({i})'
            i += 1
        return new_name

    def check_node(self, node_name: str) -> Folder | Url:
        """Check if the node is in the global nodes dictionary

        :raises NodeNotExists if node_name does not exist

        :param node_name: name of a node
        :return: the object of node_name, Folder or Url
        """
        if node_name in self.nodes_dict:
            return self.nodes_dict[node_name]  # the node exists, return the node object
        else:
            raise exceptions.NodeNotExists(node_name)  # a named node does not exist, NodeNotExist error

    def get_node(self, node_name: str) -> dict:
        """Get a node content.
        Replace children objects with their names for folder children list

        :raises raise NodeNotExists if node_name does not exist

        :param node_name: node name
        :return: dictionary {field_name: field_value} of the node
        """
        node_object = self.check_node(node_name)  # get the node instance or NodeNotExist error
        node_content = node_object.__dict__.copy()  # local copy of the node's dict

        # ---- check if the node is a folder ----
        if 'children' in node_content:  # any folder has a children list
            # ---- replace objects with their names ----
            children_list = [x.name for x in node_content['children']]  # get children names
            node_content['children'] = children_list  # put children names instead of objects
        return node_content

    def add_node(self, attr_dict: dict, node_type: bool):
        """Add a folder or url to the tree.

        :param attr_dict: dictionary with initial node attributes
        :param node_type: True for folder adding, False for url
        :return: nothing
        """
        new_node: Folder | Url  # explicit type declaration for mypy checking

        # replace the parent name with the parent guid
        parent_node = self.nodes_dict[attr_dict['parent_name']]  # get a parent object
        attr_dict['parent_guid'] = parent_node.__dict__['guid']  # get the parent guid and set it to args
        del attr_dict['parent_name']  # remove the unnecessary argument

        # create a node, folder or url
        if node_type:
            new_node = Folder(**attr_dict)  # create a new folder instance
        else:
            new_node = Url(**attr_dict)  # create a new url instance

        # modify the parent's children list and common nodes dict
        parent_node.children.append(new_node)  # add new node object to the parent child list
        self.nodes_dict[new_node.name] = new_node  # add new node object to the node's dict

    def update_node(self, name: str, attr_dict: dict):
        """Update a folder or url of the internal tree.

        :param name: updating node name
        :param attr_dict: dictionary with the updating fields
        :return: nothing
        """
        node_object = self.nodes_dict[name]  # get the node object

        # update of the parent node's date_modified field
        parent_folder = self.get_parent(name)  # get the parent folder object
        today = datetime.today().replace(microsecond=0)  # get today datetime object
        parent_folder.date_modified = datetime.isoformat(today)  # insert the current datetime as a string

        node_object.update(**attr_dict)  # update a node instance

        if name != attr_dict['name']:
            # a node name should be changed, update the common node's dict
            del self.nodes_dict[name]  # delete old (name: obj) pair from the node's dict
            self.nodes_dict[attr_dict['name']] = node_object  # add updated node object to the node's dict

    def delete_node(self, name: str):
        """Delete a node from the current tree.

        :raises NodeNotExists: if node_name does not exist
        :raises FolderNotEmpty: if node_name folder is not empty

        :param name: node name to delete
        :return: nothing
        """
        node_object = self.check_node(name)  # get the node instance if the node exists or raise NodeNotExist

        if 'children' in node_object.__dict__ and node_object.__dict__['children']:  # child list presents and non-empty
            raise exceptions.FolderNotEmpty(name)  # can not delete a non-empty folder, raise FolderNotEmpty

        # find a list of children of the parent node and delete the reference to the deleted node
        parent_guid = node_object.parent_guid  # get the parend guid of the node
        for x in self.nodes_dict.values():  # search within the common dict of nodes
            if x.guid == parent_guid:
                children = x.children  # get the child list of the parent node
                children.remove(node_object)  # delete the node's object from the parent's child list
        del self.nodes_dict[name]  # remove the node from global node dict

    def get_parent(self, node_name: str) -> Folder:
        """Get a parent node object of the current node

        :raises NodeNotExists: if node_name does not exist

        :param node_name: current node name
        :return: parent node object
        """
        node_object = self.check_node(node_name)  # get the node instance if the node exists or raise NodeNotExist
        parent_guid = node_object.parent_guid  # get the parend guid of the node
        # create a set of folders whose GUID is equal to the parent GUID (actually only one item)
        # and get the first (and unique) element of a such set
        return next(iter({x for x in self.nodes_dict.values() if x.guid == parent_guid}))
