"""Interface between Presenter class and Model implementations."""

import typing as t

class ModelProto(t.Protocol):
    """Prototype class of Model.

    """

    cwd: str  #: current work directory

    # ---- nodes section ----
    def get_children(self, node_name: str) -> tuple[bool, tuple[str, ...]]:
        """Get a list of child names of the node.

        :exceptions: NodeNotExists if node_name does not exist

        :param node_name: name of a node
        :return: True/False, tuple of child's names/empty tuple
        """

    def add_node(self, attr_dict: dict, node_type: bool):
        """Add a folder or url to the tree and save the tree into the file

        :param attr_dict: dictionary with initial node attributes
        :param node_type: True for folder adding, False for url
        :return: nothing
        """

    def update_node(self, name: str, attr_dict: dict):
        """Update a folder or url of the internal tree and save it into the file

        :param name: updating node name
        :param attr_dict: dictionary with the updating fields
        :return: nothing
        """

    def delete_node(self, name: str):
        """Delete a node from the current tree.

        :raises NodeNotExists: if node_name does not exist
        :raises FolderNotEmpty: if node_name folder is not empty

        :param name: node name to delete
        :return: nothing
        """

    def get_node(self, name: str) -> dict:
        """Get a node content.
        Replace children objects with their names for folder children list

        :param name: node name
        :return: dictionary {field_name: field_value} of the node
        """

    # ---- database section ----
    def create_database(self, name: str):
        """Create an empty bookmark structure and a file to keep the database.

        :exceptions: FileExistsError if given filename exists

        :param name: name of the new database, filename for it
        :return: nothing
        """

    def open_database(self, name: str):
        """Open a database, read and extract it into a bookmark tree.

        :exception: FileNotFoundError if the filename does not exist

        :param name: name and filename of the deleting database
        :return: nothing
        """

    def delete_database(self, name):
        """Delete the database file.

        :exception: FileNotFoundError if the filename does not exist

        :param name: name and filename of the deleting database
        :return: nothing
        """

    # ---- convertors section ----
    def convert_chrome(self, filename: str) -> tuple[bool, str]:
        """Convert Chrome bookmark JSON filename to the current tree. Return (True/False, error message)

        :param filename: Google bookmark filename to convert
        :return: (True, empty string)  or (False, error message)
        """

    def convert_mozilla(self, filename: str) -> tuple[bool, str]:
        """Convert Mozilla bookmark filename to the current tree. Return (True/False, error message).

        :param filename: Mozilla bookmark filename to convert
        :return: (True, empty string)  or (False, error message)
        """

class Model:
    """Model class."""

    def __init__(self, proto: ModelProto):
        self.proto = proto
        self.cwd = proto.cwd  #: current work directory

        # ---- nodes section ----

    def get_children(self, node_name: str) -> tuple[bool, tuple[str, ...]]:
        """Get a list of child names of the node.

        :exceptions: NodeNotExists if node_name does not exist

        :param node_name: name of a node
        :return: True/False, tuple of child's names/empty tuple
        """
        return self.proto.get_children(node_name)

    def add_node(self, attr_dict: dict, node_type: bool):
        """Add a folder or url to the tree and save the tree into the file

        :param attr_dict: dictionary with initial node attributes
        :param node_type: True for folder adding, False for url
        :return: nothing
        """
        self.proto.add_node(attr_dict, node_type)

    def update_node(self, name: str, attr_dict: dict):
        """Update a folder or url of the internal tree and save it into the file

        :param name: updating node name
        :param attr_dict: dictionary with the updating fields
        :return: nothing
        """
        self.proto.update_node(name, attr_dict)

    def delete_node(self, name: str):
        """Delete a node from the current tree.

        :raises NodeNotExists: if node_name does not exist
        :raises FolderNotEmpty: if node_name folder is not empty

        :param name: node name to delete
        :return: nothing
        """
        self.proto.delete_node(name)

    def get_node(self, name: str) -> dict:
        """Get a node content.
        Replace children objects with their names for folder children list

        :param name: node name
        :return: dictionary {field_name: field_value} of the node
        """
        return self.proto.get_node(name)

    # ---- database section ----
    def create_database(self, name: str):
        """Create an empty bookmark structure and a file to keep the database.

        :exceptions: FileExistsError if given filename exists

        :param name: name of the new database, filename for it
        :return: nothing
        """
        self.proto.create_database(name)

    def open_database(self, name: str):
        """Open a database, read and extract it into a bookmark tree.

        :exception: FileNotFoundError if the filename does not exist

        :param name: name and filename of the deleting database
        :return: nothing
        """
        self.proto.open_database(name)

    def delete_database(self, name):
        """Delete the database file.

        :exception: FileNotFoundError if the filename does not exist

        :param name: name and filename of the deleting database
        :return: nothing
        """
        self.proto.delete_database(name)

    # ---- convertors section ----
    def convert_chrome(self, filename: str) -> tuple[bool, str]:
        """Convert Chrome bookmark JSON filename to the current tree. Return (True/False, error message)

        :param filename: Google bookmark filename to convert
        :return: (True, empty string)  or (False, error message)
        """
        return self.proto.convert_chrome(filename)

    def convert_mozilla(self, filename: str) -> tuple[bool, str]:
        """Convert Mozilla bookmark filename to the current tree. Return (True/False, error message).

        :param filename: Mozilla bookmark filename to convert
        :return: (True, empty string)  or (False, error message)
        """
        return self.proto.convert_mozilla(filename)
