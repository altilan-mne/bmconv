"""A Presenter module for bookmarks manager program within MVP pattern.
Contains a class Presenter and the main() routine. Uses interface approach for isolation
of the Presenter business logic from View and Model parts of the pattern.
Creates, opens and converts bookmark files of internet browsers to the internal structure.
Allows to add, delete, and modify bookmark nodes, which are folders and URLs.

"""
import sys
import traceback
import typing as t

import exceptions  # user exceptions
from model_interface import Model
from model_sqlite import ModelSQLite  # connection to the Model part of the pattern

from view_interface import View
from view_cli import ViewCLI  # connection to the View part of the pattern

from common import VALID_CHARS, URL_FIELDS, FOLDER_FIELDS  # constants
from common import MenuItem, Field  # user types


class Presenter:
    """A class that contains the main logic of the bookmark manager.

    :param: No Parameters
    """

    def __init__(self):
        """Constructor method.
`
        """
        self.START_MENU = (
            MenuItem("Create a new bookmark's tree", self.create_tree),
            MenuItem("Open the bookmark's tree", self.open_tree),
            MenuItem("Convert external bookmark database to the internal format", self.convert_from),
            MenuItem("Exit", self.exit_of_loop),
        )  # start menu to open or create the current bookmark structure

        self.MAIN_MENU = (
            MenuItem("Create a new bookmark's tree", self.create_tree),
            MenuItem("Open the bookmark's tree", self.open_tree),
            MenuItem("Convert external bookmark database to the internal format", self.convert_from),
            MenuItem("Add a new node to the current tree, folder of url", self.add_bookmark),
            MenuItem("Modify the node of the current tree, folder or url", self.modify_bookmark),
            MenuItem("Delete the node of the current tree, folder or url", self.delete_bookmark),
            MenuItem("Print the current bookmark tree", self.print_tree),
            MenuItem("Exit", self.exit_of_loop),
        )  # main menu

        self.view = View(ViewCLI())  # instance of a View implementation, here for CLI terminal
        self.menu_items: tuple[MenuItem, ...] = self.START_MENU    # prepare for start main menu
        self.model = Model(ModelSQLite()) # instance of a Model implementation, here for SQLite version

    # ---- begin of the execution methods section ----
    @staticmethod
    def exit_of_loop() -> tuple[bool, str]:
        """Exit from the executive menu, empty argument for compatibility

        :return: False, epmty string, for compatibility
        """
        return False, ''  # for compatibility

    def create_tree(self) -> bool:
        """Create a new bookmark's structure.
        Request a filename for the new structure (alphabetic and numeric characters, additional
        characters from VALID_CHARS only) and open a file for the new bookmark set.
        If the file already exists, request to overwrite this file.

        :return: True for success otherwise False
        """
        self.view.output_header(self.view.main_header)     # print the header of the action
        prompt = "Input a name of a new bookmark's tree"    # set a prompt for the name request
        name = self.view.input_line(prompt, VALID_CHARS)   # get the bookmark's tree name
        if name is None:
            return False  # break
        try:
            self.model.create_database(name)    # create a new database file
        except FileExistsError:     # this file already exists
            except_message = f'File <{name}> already exists in the work directory {self.model.cwd}'
            self.view.output_string(except_message)
            prompt = f'Do you want to overwrite bookmark tree <{name}>? All data will be lost... (Yes/No)'  # ask Y/N
            if not self.view.input_yes_or_no(prompt):  # if not
                self.view.output_string(f'Keep an existing bookmark tree <{name}>'
                                        f' {chr(10)}')  # chr(10) instead of '\n', which is not possible in the f-string
                return False  # to the main menu
            self.model.delete_database(name)   # delete existing db and file before
            self.model.create_database(name)  # create a new database file
        self.menu_items = self.MAIN_MENU    # if file creating was ok - set the full main menu
        self.view.output_string(f'Current database is <{name}> {chr(10)}')  # output the current db name
        return True

    def open_tree(self) -> bool:
        """Open an existing bookmark structure from a file.

        :return:  True for success otherwise False
        """
        self.view.output_header(self.view.main_header)  # print the header of the action
        prompt = "Input the name of the bookmark's tree"  # set a prompt for the name request
        name = self.view.input_line(prompt, VALID_CHARS)  # get the bookmark's filename
        if name is None:
            return False  # break
        try:
            self.model.open_database(name)  # open database file
        except FileNotFoundError:
            except_message = f'File <{name}> does not exist in the work directory {self.model.cwd}{chr(10)}'
            self.view.output_string(except_message)     # output an error
            return False  # to the main menu
        self.menu_items = self.MAIN_MENU    # if ok - set the full main menu
        self.view.output_string(f'Current database is <{name}> {chr(10)}')  # output the current db name
        return True

    def add_bookmark(self) -> bool:
        """Add a new node to the current tree, folder of url.
        Request a name for a new bookmark (alphabetic and numeric characters, additional
        characters from VALID_CHARS only). Duplicate names will be rejected.
        Request the name of the parent folder to add the bookmark and check if it exists.
        Request of type of the new bookmark: folder or url.
        Get the values for the url type bookmark fields.


        :return: True for success otherwise False
        """
        self.view.output_header(self.view.main_header)  # print the header
        attr_dict = {}    # clear a dict of attrs of a new node

        # ---- name request ----
        prompt = "Input a name of the new bookmark"  # set a prompt for the name request
        name = self.view.input_line(prompt, VALID_CHARS)  # get the bookmark name
        if name is None or not name:
            return False  # break
        # check duplicate names here
        try:
            result, data = self.model.get_children(name)
            message = f'Bookmark <{name}> already exists {chr(10)}'
            self.view.output_string(message)  # output an error message
            return False  # to the main menu
        except exceptions.NodeNotExists:
            pass  # ok, new node name is unique
        attr_dict['name'] = name    # set a name of the new node

        # ---- parent folder request and check if it exists ----
        prompt = "Input a name of the parent folder for a new bookmark"  # set a prompt for the parent name request
        name = self.view.input_line(prompt, VALID_CHARS)  # get a parent folder name
        if name is None:
            return False  # break

        try:
            result, data = self.model.get_children(name)  # get children names if they present
        except exceptions.NodeNotExists as e:
            self.view.output_string(str(e))  # output error if name doesn't exist
            return False  # to the main menu
        # check if it is a folder, returned (True/False, children names/empty)
        if result:   # if the named folder exists
            attr_dict['parent_name'] = name     # set a parent node name of the new node
        else:
            message = f'Node <{name}> is not a folder {chr(10)}'
            self.view.output_string(message)  # output a success message
            return False  # to the main menu

        # ---- type request: folder or url ----
        prompt = f'Do you want to add a folder (yes), otherwise an url bookmark (no)? (Yes/No)'
        node_type = self.view.input_yes_or_no(prompt)  # add a folder if True, an url if False
        if not node_type:   # that's an url, get additional args
            # get an url attribute
            prompt = "Input an URL for the new bookmark"  # set a prompt for the URL request
            res = self.view.input_line(prompt)
            if res is None:
                return False  # break
            attr_dict['url'] = res

            # get an icon attribute
            prompt = "Input an icon for the new bookmark"  # set a prompt for the icon request
            res = self.view.input_line(prompt)
            if res is None:
                return False  # break
            attr_dict['icon'] = res

            # get a keywords attribute
            prompt = "Input keywords for the new bookmark"  # set a prompt for the keywords request
            res = self.view.input_line(prompt)
            if res is None:
                return False  # break
            attr_dict['keywords'] = res

        self.model.add_node(attr_dict, node_type)    # add the node to the tree
        message = f'Folder/Url <{attr_dict["name"]}> has been added {chr(10)}'
        self.view.output_string(message)  # output a success message
        return True

    def modify_bookmark(self) -> bool:
        """Modify the fields of the existing tree node, folder or url.
        Select a node, then select a field of node to modify.
        Return when the field was modified or selection was broken by user.
        Editable fields for url are contained in URL_FIELDS, for folders - in FOLDER_FIELDS.

        :return: True for success otherwise False
        """
        result: t.Optional[bool]  # explicit type declaration for mypy
        node_name: str = 'roots'  # initial folder of the tree
        node_stack: list[str] = []  # stack of the folders
        self.view.output_header(self.view.main_header)  # output menu header
        while True:
            # node selection loop
            try:
                result, item_list = self.model.get_children(node_name)  # get children names of the node
                if not result:  # False
                    # an url has been selected
                    try:
                        attr_dict = self.model.get_node(node_name)  # get all attributes of the node if it exists
                    except exceptions.NodeNotExists as e:
                        traceback.print_exception(e, file=sys.stdout)  # traceback output
                        sys.exit(1)  # stop execution with an error
                    # ok, continue
                    filtered_attrs = {key: attr_dict[key] for key in URL_FIELDS}  # a dict of editable fields

                    # edit the filtered attributes of the selected node
                    # select a field to edit
                    copy_filtered_attrs = filtered_attrs.copy()  # keep mutable arg for pytest
                    result, selected_field = self.view.select_field(copy_filtered_attrs, node_name)  # field request

                    match result, selected_field:  # parsing results of selection
                        case [None, _]:
                            return False # break, return to main menu
                        case [False, _]:  # return to the node selection
                            node_name = node_stack.pop()  # get the previous node name from the node's stack
                            result, item_list = self.model.get_children(node_name)  # get children names

                        case [True, _]:  # node has been selected
                            editing_field = Field(selected_field, filtered_attrs[selected_field])  # tuple(name, value)
                            new_field = self.view.edit_field(editing_field)  # edit the selected field of the node
                            if new_field is None:
                                return False  # break, return to main menu
                            filtered_attrs[selected_field] = new_field  # update attrs of node
                            self.model.update_node(node_name, filtered_attrs)  # update the node in the tree
                            message = f'Folder/Url <{node_name}> has been modified {chr(10)}'
                            self.view.output_string(message)  # output a success message
                            return True

                        case _:
                            message = f'Selection Error. Unexpected result <{result},' \
                                      f' {selected_field}> has been encountered {chr(10)}'
                            self.view.output_string(message)  # output a error message
                            return False

            except exceptions.NodeNotExists as e:
                self.view.output_string(str(e))  # named folder doesn't exist, output an error message
                return False

            # a folder was selected
            comm_list = ('Return to the previous selection',
                         'Modify current node')  # the commands for selection list
            header1 = self.view.main_header  # get the current main header
            header2 = f'Select a node in the folder <{node_name}> or a command.'  # set a sub-header
            result, selected_node = self.view.select_item(item_list, comm_list,
                                                          header1=header1, header2=header2)  # get an item or command
            match result, selected_node:  # parsing results of selection
                case [None, _]:
                    return False  # break, return to main menu
                case [False, 0]:  # return to the parent folder or to main menu from roots
                    if node_name == 'roots':  # out of the folder <roots>
                        return True  # to the main menu
                    if node_stack:
                        node_name = node_stack.pop()  # get the parent folder name from the node's stack
                case [False, 1]:  # modify this folder
                    try:
                        attr_dict = self.model.get_node(node_name)  # get all attributes of the node if it exists
                    except exceptions.NodeNotExists as e:
                        traceback.print_exception(e, file=sys.stdout)  # traceback output
                        sys.exit(1)  # stop execution with an error
                    # ok, continue
                    filtered_attrs = {key: attr_dict[key] for key in FOLDER_FIELDS}  # a dict with only required fields
                    copy_filtered_attrs = filtered_attrs.copy()  # keep mutable arg for pytest
                    # select a folder field
                    result, selected_field = self.view.select_field(copy_filtered_attrs, node_name)  # selection request
                    match result, selected_field:  # parsing results of selection
                        case [None, _]:
                            return False  # break, return to main menu
                        case [False, _]:  # return to the node selection
                            node_name = node_stack.pop()  # get the previous node name from the node's stack
                            result, item_list = self.model.get_children(node_name)  # get children names
                        case [True, _]:  # node has been selected
                            if filtered_attrs[selected_field] != 'roots':
                                editing_field = Field(selected_field,
                                                      filtered_attrs[selected_field])  # tuple(name, value)
                                new_field = self.view.edit_field(editing_field)  # edit the selected field of the node
                                if new_field is None:
                                    return False  # break, return to main menu
                                filtered_attrs[selected_field] = new_field  # update attrs of node
                                self.model.update_node(node_name, filtered_attrs)  # update the node in the tree
                                node_name = filtered_attrs['name']  # get the new folder name
                            else:
                                message = f'Node <roots> can not be renamed. {chr(10)}'
                                self.view.output_string(message)  # output a error message

                case [True, _]:  # a folder has been selected, nested selection
                    node_stack.append(node_name)  # put a previous node name to the node's stack
                    node_name = item_list[selected_node]  # get a new node name from item_list
                case _:
                    message = f'Selection Error. Unexpected result <{result},' \
                              f' {selected_node}> has been encountered {chr(10)}'
                    self.view.output_string(message)  # output a error message
                    return False

    def delete_bookmark(self) -> bool:
        """Delete the node of the current tree, folder or url.
        Request the name of the node to delete, check if it exist.
        Node <roots> can not be deleted.
        Non-empty folder can not be deleted.

        :return: True for success otherwise False
        """
        self.view.output_header(self.view.main_header)  # print the header

        # ---- name request ----
        prompt = "Input the name of the bookmark"  # set a prompt for the name request
        name = self.view.input_line(prompt, VALID_CHARS)  # get the bookmark name
        if name is None or not name:
            return False  # break
        # ---- root folder can't be deleted ----
        if name == 'roots':
            self.view.output_string(f'Folder <{name}> can not be deleted {chr(10)}')
            return False

        # ---- check if exists and if it's ok then delete the node or error output
        try:
            self.model.delete_node(name)  # delete the node
        except (exceptions.NodeNotExists, exceptions.FolderNotEmpty) as e:
            self.view.output_string(str(e))  # named folder doesn't exist or not empty, output an error message
            return False
        else:
            message = f'Bookmark {name} has been deleted {chr(10)}'
            self.view.output_string(message)  # output a success message
            return True

    def print_tree(self) -> bool:
        """Print the names of all the bookmark nodes of the current tree.
        Use recursive inner function _output_loop().

        :return: True for success otherwise False
        """
        def _output_loop(node_name, tab):
            result, child_names = self.model.get_children(node_name)  # get children names of the <node_name>
            if result:  # node has children, this is a folder
                self.view.output_header(f'Folder <{node_name}> BEGIN', tab)  # BEGIN of the folder <node_name>
                tab += 8  # increment of tab shift
                for child in child_names:
                    _output_loop(child, tab)  # it is a nested folder, call it recursively
                self.view.output_header(f'Folder <{node_name}> END', tab - 8)  # END of the folder <node_name>
            else:
                # print a url name
                self.view.output_list((node_name, ), tab)  # output url name with current tabulation

        # ---- body of print_tree()
        self.view.output_header(self.view.main_header)  # print the header
        init_node = 'roots' # start folder
        init_tab = 0  # start indent
        _output_loop(init_node, init_tab)  # load initial node name and tabulation for recursion
        return True

    def convert_from(self) -> bool:
        """Convert external bookmark database to the internal format.
        Request a name of the external source file, check if it exists.
        Request a filename of the new internal bookmark structure and create it.
        Request a format of the external source and convert this file.


        :return: True for success otherwise False
        """
        self.view.output_header(self.view.main_header)  # print the header

        # ---- get source file name and check if the source file exists ----
        prompt = "Input a name of the external source file"  # set a prompt for the name request of the source
        filename = self.view.input_line(prompt)  # get an external source file name
        if filename is None:
            return False  # to the main menu, break
        try:
            with open(filename, 'r') as f:  # open the source file, or FileNotFoundError exception
                pass
        except FileNotFoundError:
            except_message = f'File "{filename}" not exists in the work directory {self.model.cwd}'
            self.view.output_string(except_message)     # output an error
            return False  # to the main menu

        # ---- create a new database as the destination for conversation ----
        result = self.create_tree()
        if not result:
            return False  # to the main menu, break

        # ---- request a format of the source file: Chrome, Mozilla or smth. else ----
        menu_items = (
             MenuItem("Convert from Chrome JSON format", self.model.convert_chrome),
             MenuItem("Convert from Mozilla format", self.model.convert_mozilla),
             MenuItem("Exit", self.exit_of_loop)
        )   # a local menu for format selection

        menu_item = self.get_request(menu_items)  # get a menu item
        if menu_item is None:
            return True  # return to the upper menu

        self.view.output_string(f'Convert external file <{filename}> to the tree')  # output the message

        result, data = menu_item.call(filename)  # execute the selected method, source filename as param
        if result:  # conversation was successful
            self.view.output_string(f'File <{filename}> was converted to the current tree {chr(10)}')  # output ok
            return True
        else:
            self.view.output_string(data)  # output an error message of the result
            return False

    # ---- end of the execution methods section ----

    def get_request(self, menu_items: tuple[MenuItem, ...]) -> t.Optional[MenuItem]:
        """Get a user request from menu options. Menu is a tuple."""
        item_list = tuple([item.descr for item in menu_items])  # prepare a list of items
        selected_no = self.view.select_line(item_list)   # get the selected menu number
        if selected_no is None:
            return None  # break
        else:
            return menu_items[selected_no]  # return the selected menu item

    def execute_request(self, menu_item: MenuItem):
        """Call the instance method from the current menu"""
        self.view.main_header = menu_item.descr  # set description of a routine to the global var
        result = menu_item.call()  # call a routine, return True if successful otherwise False


def main():
    """Main routine of the bookmark manager.
    Create an instance of the class Presenter.
    An infinite loop of the main menu until the EOF and RETURN or <Exit> commands are executed.

    :return: no
    """
    request_handler = Presenter()
    # --------------------------------------------------
    while True:
        request_handler.view.output_header('Main menu')
        menu_item = request_handler.get_request(request_handler.menu_items)
        if menu_item is None or menu_item.descr == 'Exit':
            break
        request_handler.execute_request(menu_item)
    # end of the main loop
    print('Thank you and goodbye!')

if __name__ == '__main__':
    main()
