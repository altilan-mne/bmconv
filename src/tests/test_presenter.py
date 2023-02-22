"""Tests for a Presenter module"""

import os
import sys
from unittest import mock

import exceptions as e
from view_interface import View
from model_interface import Model
from presenter import Presenter

from common import VALID_CHARS
from common import Field

class TestPresenter:
    """Testing class for a Presenter module."""

    pres = Presenter()

    def test_init_class(self):
        """Test of instance initialisation."""
        assert isinstance(self.pres.view, View)
        assert isinstance(self.pres.model, Model)
        assert self.pres.menu_items == self.pres.START_MENU

    def test_exit_of_loop(self):
        assert self.pres.exit_of_loop() == (False, '')

    def test_create_tree(self):
        # ---- a successful case ----
        correct_name = 'New database'  # correct name of a new database
        cwd = os.getcwd()
        # mock methods
        self.pres.view = mock.MagicMock(name='view', spec=View)  # mock View interface
        self.pres.model = mock.MagicMock(name='model', spec=Model)  # mock Model interface

        self.pres.view.main_header = "TEST HEADER CREATE TREE"  # set a mocking method header
        self.pres.model.cwd = cwd  # current directory for test
        self.pres.view.input_line.return_value = correct_name  # mock a user input

        result = self.pres.create_tree()  # test of the method

        # test results
        assert self.pres.view.output_header.call_args.args == (self.pres.view.main_header,)  # header output
        assert self.pres.view.input_line.call_args.args == (
            "Input a name of a new bookmark's tree",
            VALID_CHARS)
        assert self.pres.model.create_database.call_count == 1
        assert self.pres.model.delete_database.call_count == 0
        assert self.pres.model.create_database.call_args.args == (correct_name,)
        assert self.pres.menu_items == self.pres.MAIN_MENU
        assert self.pres.view.output_string.call_args.args == (f'Current database is <{correct_name}> {chr(10)}',)
        assert result is True  # success

        # ---- a break or incorrect name case ----
        self.pres.view.reset_mock()
        self.pres.model.reset_mock()
        self.pres.view.input_line.return_value = None  # mock a user input

        result = self.pres.create_tree()  # test of the method

        assert self.pres.model.create_database.call_count == 0
        assert result is False  # not success

        # ---- the database exists, overwrite case ----
        # mock methods
        self.pres.view.reset_mock()
        self.pres.model.reset_mock()
        self.pres.view.input_line.return_value = correct_name  # mock a user input
        self.pres.model.create_database.side_effect = [FileExistsError, None]  # 1st - exception, 2nd - None
        self.pres.view.input_yes_or_no.return_value = True

        result = self.pres.create_tree()  # test of the method

        assert self.pres.view.input_yes_or_no.call_args.args == (
            f'Do you want to overwrite bookmark tree <{correct_name}>? All data will be lost... (Yes/No)',)
        assert self.pres.model.create_database.call_count == 2
        assert self.pres.model.delete_database.call_count == 1
        assert self.pres.model.create_database.call_args.args == (correct_name,)
        assert self.pres.menu_items == self.pres.MAIN_MENU
        exp_str_1 = f'File <{correct_name}> already exists in the work directory {self.pres.model.cwd}'
        exp_str_2 = f'Current database is <{correct_name}> {chr(10)}'
        assert self.pres.view.output_string.call_args_list == [((exp_str_1,),), ((exp_str_2,),)]
        assert result is True  # success

        # ---- the database exists, keeping case ----
        # mock methods
        self.pres.view.reset_mock()
        self.pres.model.reset_mock()
        self.pres.model.create_database.side_effect = FileExistsError  # exception only
        self.pres.view.input_yes_or_no.return_value = False

        result = self.pres.create_tree()  # test of the method
        # test results
        assert self.pres.view.input_yes_or_no.call_args.args == (
            f'Do you want to overwrite bookmark tree <{correct_name}>? All data will be lost... (Yes/No)',)
        assert self.pres.model.create_database.call_count == 1
        assert self.pres.model.delete_database.call_count == 0
        exp_str_1 = f'File <{correct_name}> already exists in the work directory {self.pres.model.cwd}'
        exp_str_2 = f'Keep an existing bookmark tree <{correct_name}> {chr(10)}'
        assert self.pres.view.output_string.call_args_list == [((exp_str_1,),), ((exp_str_2,),)]
        assert result is False  # success

    def test_open_tree(self):
        # ---- a successful case ----
        correct_name = 'Opening database'  # correct name of a new database
        cwd = os.getcwd()
        # mock methods
        self.pres.view = mock.MagicMock(name='view', spec=View)  # mock View interface
        self.pres.model = mock.MagicMock(name='model', spec=Model)  # mock Model interface

        self.pres.view.main_header = "TEST HEADER OPEN TREE"  # set a mocking method header
        self.pres.model.cwd = cwd  # current directory for test
        self.pres.view.input_line.return_value = correct_name  # mock a user input

        result = self.pres.open_tree()  # test of the method

        # test results
        assert self.pres.view.output_header.call_args.args == (self.pres.view.main_header,)  # header output
        assert self.pres.view.input_line.call_args.args == (
            "Input the name of the bookmark's tree",
            VALID_CHARS)
        assert self.pres.model.open_database.call_count == 1
        assert self.pres.model.open_database.call_args.args == (correct_name,)
        assert self.pres.menu_items == self.pres.MAIN_MENU
        assert self.pres.view.output_string.call_args.args == (f'Current database is <{correct_name}> {chr(10)}',)
        assert result is True  # success

        # ---- a break or incorrect name case ----
        self.pres.view.reset_mock()
        self.pres.model.reset_mock()
        self.pres.view.input_line.return_value = None  # mock a user input

        result = self.pres.open_tree()  # test of the method

        assert self.pres.model.open_database.call_count == 0
        assert result is False  # not success

        # ---- the database does not exist ----
        # mock methods
        self.pres.view.reset_mock()
        self.pres.model.reset_mock()
        self.pres.view.input_line.return_value = correct_name  # mock a user input
        self.pres.model.open_database.side_effect = FileNotFoundError  # exception

        result = self.pres.open_tree()  # test of the method

        assert self.pres.model.open_database.call_count == 1
        assert self.pres.model.open_database.call_args.args == (correct_name,)

        exp_str = f'File <{correct_name}> does not exist in the work directory {self.pres.model.cwd}{chr(10)}'
        assert self.pres.view.output_string.call_args.args == (exp_str,)
        assert result is False  # error

    def test_add_bookmark_folder(self):
        # ---- common params ----
        new_node = 'New folder'  # correct name of a new folder
        parent_node = 'Parent folder'
        get_children_none = False, ()
        get_children_folder = True, ('item1', 'item2')
        attr_dict = {'name': new_node, 'parent_name': parent_node}  # args to add node
        node_type = True

        # ---- add a folder successfully ----
        # mock methods
        self.pres.view = mock.MagicMock(name='view', spec=View)  # mock View interface
        self.pres.model = mock.MagicMock(name='model', spec=Model)  # mock Model interface
        self.pres.view.main_header = "TEST HEADER ADD BOOKMARK"  # set a mocking method header
        # input_line params
        self.pres.view.input_line.side_effect = [new_node, parent_node]  # 2 calls for this case
        prompt_1 = "Input a name of the new bookmark", VALID_CHARS
        prompt_2 = "Input a name of the parent folder for a new bookmark", VALID_CHARS
        # yes_or_no params
        self.pres.view.input_yes_or_no.return_value = True  # folder for this case
        yes_or_no_prompt = f'Do you want to add a folder (yes), otherwise an url bookmark (no)? (Yes/No)'
        # get_children params
        self.pres.model.get_children.side_effect = [e.NodeNotExists(new_node), get_children_folder]  # 2 calls

        result = self.pres.add_bookmark()  # test of the method

        assert self.pres.view.output_header.call_args.args == (self.pres.view.main_header,)  # header output
        assert self.pres.view.input_line.call_args_list == [(prompt_1,), (prompt_2,), ]  # input_line args
        assert self.pres.view.input_yes_or_no.call_args.args == (yes_or_no_prompt,)  # yes_or_no arg
        assert self.pres.model.get_children.call_count == 2
        assert self.pres.model.add_node.call_count == 1
        assert self.pres.model.add_node.call_args.args == (attr_dict, node_type)
        assert result is True

        # ---- break at new node input ----
        self.pres.view.reset_mock(side_effect=True)  # reset side_effect from the previous case
        self.pres.model.reset_mock()
        self.pres.view.input_line.return_value = None  # break for this case
        result = self.pres.add_bookmark()  # test of the method
        assert result is False

        # ---- wrong nome of a new node ----
        self.pres.view.reset_mock()  # reset side_effect from the previous case
        self.pres.model.reset_mock()
        self.pres.view.input_line.return_value = ''  # invalid chars in the name
        result = self.pres.add_bookmark()  # test of the method
        assert result is False

        # ---- new node already exists ----
        self.pres.view.reset_mock()  # reset
        self.pres.model.reset_mock(side_effect=True)  # reset
        self.pres.view.input_line.return_value = new_node  # valid new name
        # get_children params
        self.pres.model.get_children.return_value = get_children_none  # name exists

        result = self.pres.add_bookmark()  # test of the method
        exp_str = f'Bookmark <{new_node}> already exists {chr(10)}'
        assert self.pres.view.output_string.call_args.args == (exp_str,)
        assert result is False

    def test_add_bookmark_url(self):
        # ---- common params ----
        new_node = 'New url'  # correct name of a new url
        parent_node = 'Parent folder'
        url = 'URL'
        icon = 'ICON'
        keywords = 'KEYWORDS'
        get_children_none = False, ()
        get_children_folder = True, ('item1', 'item2')
        attr_dict = {'name': new_node, 'parent_name': parent_node,
                     'url': url, 'icon': icon, 'keywords': keywords}  # args to add node
        node_type = False

        # ---- add a folder successfully ----
        # mock methods
        self.pres.view = mock.MagicMock(name='view', spec=View)  # mock View interface
        self.pres.model = mock.MagicMock(name='model', spec=Model)  # mock Model interface
        self.pres.view.main_header = "TEST HEADER ADD BOOKMARK"  # set a mocking method header
        # input_line params
        self.pres.view.input_line.side_effect = [new_node, parent_node, url, icon, keywords]  # 5 calls for this case
        prompt_1 = "Input a name of the new bookmark", VALID_CHARS
        prompt_2 = "Input a name of the parent folder for a new bookmark", VALID_CHARS
        prompt_3 = ("Input an URL for the new bookmark", )
        prompt_4 = ("Input an icon for the new bookmark", )
        prompt_5 = ("Input keywords for the new bookmark", )
        # yes_or_no params
        self.pres.view.input_yes_or_no.return_value = False  # an url for this case
        yes_or_no_prompt = f'Do you want to add a folder (yes), otherwise an url bookmark (no)? (Yes/No)'
        # get_children params
        self.pres.model.get_children.side_effect = [e.NodeNotExists(new_node), get_children_folder]  # 2 calls

        result = self.pres.add_bookmark()  # test of the method

        assert self.pres.view.output_header.call_args.args == (self.pres.view.main_header,)  # header output
        assert self.pres.view.input_line.call_args_list == [(prompt_1,), (prompt_2,), (prompt_3,), (prompt_4,), (prompt_5,), ]  # input_line args
        assert self.pres.view.input_yes_or_no.call_args.args == (yes_or_no_prompt,)  # yes_or_no arg
        assert self.pres.model.get_children.call_count == 2
        assert self.pres.model.add_node.call_count == 1
        assert self.pres.model.add_node.call_args.args == (attr_dict, node_type)
        assert result is True

    def test_modify_bookmark_url(self):
        # modify url
        # ---- common params ----
        node_name = 'New url'  # correct name of a new url
        parent_node = 'Parent folder'
        url = 'URL'
        icon = 'ICON'
        keywords = 'KEYWORDS'
        get_children_none = False, ()
        get_children_folder = True, ('item1', 'item2')
        selected_field = True, 'url'  # 'url' field selected
        attr_dict = {'name': node_name, 'parent_name': parent_node,
                     'url': url, 'icon': icon, 'keywords': keywords}  # args to add node
        filtered_attrs = {'name': node_name, 'url': url,
                          'icon': icon, 'keywords': keywords}  # args to add node
        new_field = 'new URL'
        new_filtered_attrs = {'name': node_name, 'url': new_field,
                              'icon': icon, 'keywords': keywords}  # args to add node
        editing_field = Field('url', filtered_attrs['url'])
        node_type = False

        # modify an url successfully
        # mock methods
        self.pres.view = mock.MagicMock(name='view', spec=View)  # mock View interface
        self.pres.model = mock.MagicMock(name='model', spec=Model)  # mock Model interface
        self.pres.view.main_header = "TEST HEADER MODIFY BOOKMARK"  # set a mocking method header
        # get_children params
        self.pres.model.get_children.return_value = get_children_none  # 1 call
        # get_node params
        self.pres.model.get_node.return_value = attr_dict
        # selected_fields params
        self.pres.view.select_field.return_value = selected_field
        # edit_field params
        self.pres.view.edit_field.return_value = new_field

        result = self.pres.modify_bookmark()  # test of the method

        assert self.pres.view.output_header.call_args.args == (self.pres.view.main_header, )
        assert self.pres.model.get_children.call_args.args == ('roots', )
        assert self.pres.model.get_node.call_args.args == ('roots', )
        assert self.pres.view.select_field.call_args.args == (filtered_attrs, 'roots', )
        assert self.pres.view.edit_field.call_args.args == (editing_field, )
        assert self.pres.model.update_node.call_args.args == ('roots', new_filtered_attrs, )
        assert self.pres.view.output_string.call_args.args == \
               (f'Folder/Url <roots> has been modified {chr(10)}',)
        assert result is True

        # modify an url with EOF break at edit field
        # mock methods
        self.pres.view.reset_mock(return_value=True)  # reset side_effect from the previous case
        self.pres.model.reset_mock(return_value=True)
        # get_children params
        self.pres.model.get_children.return_value = get_children_none  # 1 call
        # get_node params
        self.pres.model.get_node.return_value = attr_dict
        # selected_fields params
        self.pres.view.select_field.return_value = selected_field
        # edit_field params
        self.pres.view.edit_field.return_value = None  # EOF break

        result = self.pres.modify_bookmark()  # test of the method

        assert self.pres.model.get_children.call_args.args == ('roots', )
        assert self.pres.model.get_node.call_args.args == ('roots', )
        assert self.pres.view.select_field.call_args.args == (filtered_attrs, 'roots', )
        assert self.pres.view.edit_field.call_args.args == (editing_field, )
        assert result is False

        # modify an url with selection error
        # mock methods
        self.pres.view.reset_mock(return_value=True)  # reset side_effect from the previous case
        self.pres.model.reset_mock(return_value=True)
        # get_children params
        self.pres.model.get_children.return_value = get_children_none  # 1 call
        # get_node params
        self.pres.model.get_node.return_value = attr_dict
        # selected_fields params
        self.pres.view.select_field.return_value = "None", 'wrong selection'  # internal error

        result = self.pres.modify_bookmark()  # test of the method

        assert self.pres.model.get_children.call_args.args == ('roots', )
        assert self.pres.model.get_node.call_args.args == ('roots', )
        assert self.pres.view.select_field.call_args.args == (filtered_attrs, 'roots', )
        # assert self.pres.view.edit_field.call_args.args == (editing_field, )
        assert self.pres.view.output_string.call_args.args == \
               (f'Selection Error. Unexpected result <None,' \
                f' wrong selection> has been encountered {chr(10)}', )
        assert result is False

        # modify an url with EOF break at select field
        # mock methods
        self.pres.view.reset_mock(return_value=True)  # reset side_effect from the previous case
        self.pres.model.reset_mock(return_value=True)
        # get_children params
        self.pres.model.get_children.return_value = get_children_none  # 1 call
        # get_node params
        self.pres.model.get_node.return_value = attr_dict
        # selected_fields params
        self.pres.view.select_field.return_value = None, ''  # EOF was entered

        result = self.pres.modify_bookmark()  # test of the method

        assert self.pres.model.get_children.call_args.args == ('roots',)
        assert self.pres.model.get_node.call_args.args == ('roots',)
        assert self.pres.view.select_field.call_args.args == (filtered_attrs, 'roots',)
        assert result is False

        # modify an url with internal error <node does not exist> at get_node
        # mock methods
        self.pres.view.reset_mock(return_value=True)  # reset side_effect from the previous case
        self.pres.model.reset_mock(return_value=True)
        # get_children params
        self.pres.model.get_children.return_value = get_children_none  # 1 call
        # get_node params
        self.pres.model.get_node.side_effect = e.NodeNotExists('not exist')
        # switch next traceback output to stderr
        temp = sys.stdout
        sys.stdout = sys.stderr
        print()
        try:
            result = self.pres.modify_bookmark()  # test of the method
        except SystemExit as exc:
            assert type(exc) == SystemExit
            assert exc.code == 1
        sys.stdout = temp  # restore stdout

        # modify an url with internal error <node does not exist> at get_children
        # mock methods
        self.pres.view.reset_mock(return_value=True)  # reset side_effect from the previous case
        self.pres.model.reset_mock(side_effect=True)
        # get_children params
        self.pres.model.get_children.side_effect = e.NodeNotExists('not exist')  # 1 call

        result = self.pres.modify_bookmark()  # test of the method

        assert self.pres.view.output_string.call_args.args == \
               (f'Node <not exist> does not exist {chr(10)}', )
        assert result is False

    def test_modify_bookmark_folder(self):
        # modify folder
        # ---- common params ----
        node_name = 'New folder'  # correct name of a new url
        parent_node = 'Parent folder'
        get_children_none = False, ()
        get_children_folder = True, ('item1', 'item2')
        selected_field = True, 'name'  # 'url' field selected
        attr_dict = {'name': node_name, 'parent_name': parent_node}  # args to add node
        filtered_attrs = {'name': node_name}  # args to add node
        new_field = 'new folder'
        new_filtered_attrs = {'name': new_field}  # args to add node
        editing_field = Field('name', filtered_attrs['name'])
        select_items_args = (('item1', 'item2'), ('Return to the previous selection', 'Modify current node'))
        select_item_kwargs = {'header1': 'TEST HEADER MODIFY BOOKMARK',
                              'header2': 'Select a node in the folder <roots> or a command.'}

        # modify current folder successfully
        # mock methods
        self.pres.view.reset_mock(return_value=True)  # reset side_effect from the previous case
        self.pres.model.reset_mock(side_effect=True)
        # self.pres.view.main_header = "TEST HEADER MODIFY BOOKMARK"  # set a mocking method header
        # get_children params
        self.pres.model.get_children.side_effect = [get_children_folder, e.NodeNotExists('forced return')]  # 1 call
        # selected_item params
        self.pres.view.select_item.return_value = (False, 1)  # modify current folder
        # get_node params
        self.pres.model.get_node.return_value = attr_dict
        # select_field params
        self.pres.view.select_field.return_value = (True, 'name')
        # edit_field params
        self.pres.view.edit_field.return_value = new_field

        result = self.pres.modify_bookmark()  # test of the method

        assert self.pres.view.select_item.call_args.args == select_items_args
        assert self.pres.view.select_item.call_args.kwargs == select_item_kwargs
        assert self.pres.model.get_node.call_args.args == ('roots', )
        assert self.pres.view.select_field.call_args.args == (filtered_attrs, 'roots', )
        assert self.pres.view.edit_field.call_args.args == (editing_field, )
        assert self.pres.model.update_node.call_args.args == ('roots', new_filtered_attrs)
        assert self.pres.view.output_string.call_args.args == (f'Node <forced return> does not exist {chr(10)}', )
        assert result is False

        # modify current folder if it is 'roots' - disabled
        # ---- common params ----
        node_name = 'roots'  # correct name of a new url
        parent_node = None
        get_children_none = False, ()
        get_children_folder = True, ('item1', 'item2')
        selected_field = True, 'name'  # 'name' field selected
        attr_dict = {'name': node_name, 'parent_name': parent_node}  # args to add node
        filtered_attrs = {'name': node_name}  # args to add node
        # new_field = 'new folder'
        new_filtered_attrs = {'name': new_field}  # args to add node
        editing_field = Field('name', filtered_attrs['name'])
        select_items_args = (('item1', 'item2'), ('Return to the previous selection', 'Modify current node'))
        select_item_kwargs = {'header1': 'TEST HEADER MODIFY BOOKMARK',
                              'header2': 'Select a node in the folder <roots> or a command.'}
        # mock methods
        self.pres.view = mock.MagicMock(name='view', spec=View)  # mock View interface
        self.pres.model = mock.MagicMock(name='model', spec=Model)  # mock Model interface
        self.pres.view.main_header = "TEST HEADER MODIFY BOOKMARK"  # set a mocking method header
        # get_children params
        self.pres.model.get_children.side_effect = [get_children_folder, e.NodeNotExists('forced return')]  # 2 calls
        # selected_item params
        self.pres.view.select_item.return_value = (False, 1)  # modify current folder
        # get_node params
        self.pres.model.get_node.return_value = attr_dict
        # select_field params
        self.pres.view.select_field.return_value = (True, 'name')

        result = self.pres.modify_bookmark()  # test of the method

        assert self.pres.view.select_item.call_args.args == select_items_args
        assert self.pres.view.select_item.call_args.kwargs == select_item_kwargs
        assert self.pres.model.get_node.call_args.args == ('roots',)
        assert self.pres.view.select_field.call_args.args == (filtered_attrs, 'roots',)
        assert self.pres.view.output_string.call_args_list == \
               [((f'Node <roots> can not be renamed. {chr(10)}', ), ),
                ((f'Node <forced return> does not exist {chr(10)}', ), )]
        assert result is False

        # modify current folder EOF break at edit_field
        # ---- common params ----
        node_name = 'New folder'  # correct name of a new url
        parent_node = 'Parent folder'
        get_children_none = False, ()
        get_children_folder = True, ('item1', 'item2')
        attr_dict = {'name': node_name, 'parent_name': parent_node}  # args to add node
        filtered_attrs = {'name': node_name}  # args to add node
        new_field = 'new folder'
        editing_field = Field('name', filtered_attrs['name'])
        select_items_args = (('item1', 'item2'), ('Return to the previous selection', 'Modify current node'))
        select_item_kwargs = {'header1': 'TEST HEADER MODIFY BOOKMARK',
                              'header2': 'Select a node in the folder <roots> or a command.'}
        # mock methods
        self.pres.view.reset_mock(return_value=True)  # reset side_effect from the previous case
        self.pres.model.reset_mock(side_effect=True)
        # self.pres.view.main_header = "TEST HEADER MODIFY BOOKMARK"  # set a mocking method header
        # get_children params
        self.pres.model.get_children.return_value = get_children_folder  # 1 call
        # selected_item params
        self.pres.view.select_item.return_value = (False, 1)  # modify current folder
        # get_node params
        self.pres.model.get_node.return_value = attr_dict
        # select_field params
        self.pres.view.select_field.return_value = (True, 'name')
        # edit_field params
        self.pres.view.edit_field.return_value = None  # EOF break entered

        result = self.pres.modify_bookmark()  # test of the method

        assert self.pres.view.select_item.call_args.args == select_items_args
        assert self.pres.view.select_item.call_args.kwargs == select_item_kwargs
        assert self.pres.model.get_node.call_args.args == ('roots',)
        assert self.pres.view.select_field.call_args.args == (filtered_attrs, 'roots',)
        assert self.pres.view.edit_field.call_args.args == (editing_field,)
        assert result is False

        # modify current folder EOF break at select_field
        # ---- common params ----
        node_name = 'New folder'  # correct name of a new url
        parent_node = 'Parent folder'
        get_children_none = False, ()
        get_children_folder = True, ('item1', 'item2')
        attr_dict = {'name': node_name, 'parent_name': parent_node}  # args to add node
        filtered_attrs = {'name': node_name}  # args to add node
        new_field = 'new folder'
        editing_field = Field('name', filtered_attrs['name'])
        select_items_args = (('item1', 'item2'), ('Return to the previous selection', 'Modify current node'))
        select_item_kwargs = {'header1': 'TEST HEADER MODIFY BOOKMARK',
                              'header2': 'Select a node in the folder <roots> or a command.'}
        # mock methods
        self.pres.view.reset_mock(return_value=True)  # reset side_effect from the previous case
        self.pres.model.reset_mock(side_effect=True)
        # self.pres.view.main_header = "TEST HEADER MODIFY BOOKMARK"  # set a mocking method header
        # get_children params
        self.pres.model.get_children.return_value = get_children_folder  # 1 call
        # selected_item params
        self.pres.view.select_item.return_value = (False, 1)  # modify current folder
        # get_node params
        self.pres.model.get_node.return_value = attr_dict
        # select_field params
        self.pres.view.select_field.return_value = (None, '')
        # edit_field params
        # self.pres.view.edit_field.return_value = None  # EOF break entered

        result = self.pres.modify_bookmark()  # test of the method

        assert self.pres.view.select_item.call_args.args == select_items_args
        assert self.pres.view.select_item.call_args.kwargs == select_item_kwargs
        assert self.pres.model.get_node.call_args.args == ('roots',)
        assert self.pres.view.select_field.call_args.args == (filtered_attrs, 'roots',)
        # assert self.pres.view.edit_field.call_args.args == (editing_field,)
        assert result is False

        # modify current folder if return to the previous folder
        # ---- common params ----
        node_name = 'roots'  # correct name of a new url
        parent_node = None
        get_children_none = False, ()
        get_children_folder = True, ('item1', 'item2')
        selected_field = True, 'name'  # 'name' field selected
        attr_dict = {'name': node_name, 'parent_name': parent_node}  # args to add node
        filtered_attrs = {'name': node_name}  # args to add node
        # new_field = 'new folder'
        new_filtered_attrs = {'name': new_field}  # args to add node
        editing_field = Field('name', filtered_attrs['name'])
        select_items_args = (('item1', 'item2'), ('Return to the previous selection', 'Modify current node'))
        select_item_kwargs = {'header1': 'TEST HEADER MODIFY BOOKMARK',
                              'header2': 'Select a node in the folder <roots> or a command.'}
        # mock methods
        self.pres.view = mock.MagicMock(name='view', spec=View)  # mock View interface
        self.pres.model = mock.MagicMock(name='model', spec=Model)  # mock Model interface
        self.pres.view.main_header = "TEST HEADER MODIFY BOOKMARK"  # set a mocking method header
        # get_children params
        self.pres.model.get_children.side_effect = [get_children_folder, e.NodeNotExists('forced return')]  # 2 calls
        # selected_item params
        self.pres.view.select_item.return_value = (False, 1)  # modify current folder
        # get_node params
        self.pres.model.get_node.return_value = attr_dict
        # select_field params
        self.pres.view.select_field.return_value = (False, '')

        try:
            result = self.pres.modify_bookmark()  # test of the method
        except IndexError as exp:
            print('\nException IndexError raised successfully:', e, file=sys.stderr)


        assert self.pres.model.get_children.call_args.args == ('roots', )
        assert self.pres.view.select_item.call_args.args == select_items_args
        assert self.pres.view.select_item.call_args.kwargs == select_item_kwargs
        assert self.pres.model.get_node.call_args.args == ('roots',)
        assert self.pres.view.select_field.call_args.args == (filtered_attrs, 'roots',)
        assert result is False

        # modify current folder, case unexpected select_item
        # ---- common params ----
        node_name = 'roots'  # correct name of a new url
        parent_node = None
        get_children_none = False, ()
        get_children_folder = True, ('item1', 'item2')
        selected_field = True, 'name'  # 'name' field selected
        attr_dict = {'name': node_name, 'parent_name': parent_node}  # args to add node
        filtered_attrs = {'name': node_name}  # args to add node
        # new_field = 'new folder'
        new_filtered_attrs = {'name': new_field}  # args to add node
        editing_field = Field('name', filtered_attrs['name'])
        select_items_args = (('item1', 'item2'), ('Return to the previous selection', 'Modify current node'))
        select_item_kwargs = {'header1': 'TEST HEADER MODIFY BOOKMARK',
                              'header2': 'Select a node in the folder <roots> or a command.'}
        # mock methods
        self.pres.view = mock.MagicMock(name='view', spec=View)  # mock View interface
        self.pres.model = mock.MagicMock(name='model', spec=Model)  # mock Model interface
        self.pres.view.main_header = "TEST HEADER MODIFY BOOKMARK"  # set a mocking method header
        # get_children params
        self.pres.model.get_children.return_value = get_children_folder  # 1 call
        # selected_item params
        self.pres.view.select_item.return_value = "None", 'wrong selection'  # unexpected selection

        result = self.pres.modify_bookmark()  # test of the method

        assert self.pres.model.get_children.call_args.args == ('roots',)
        assert self.pres.view.select_item.call_args.args == select_items_args
        assert self.pres.view.select_item.call_args.kwargs == select_item_kwargs
        assert self.pres.view.output_string.call_args.args == \
               (f'Selection Error. Unexpected result <None,' \
                f' wrong selection> has been encountered {chr(10)}',)
        assert result is False

        # modify current folder, case children folder selected
        # ---- common params ----
        node_name = 'roots'  # correct name of a new url
        parent_node = None
        get_children_none = False, ()
        get_children_folder = True, ('item1', 'item2')
        selected_field = True, 'name'  # 'name' field selected
        attr_dict = {'name': node_name, 'parent_name': parent_node}  # args to add node
        filtered_attrs = {'name': node_name}  # args to add node
        # new_field = 'new folder'
        new_filtered_attrs = {'name': new_field}  # args to add node
        editing_field = Field('name', filtered_attrs['name'])
        select_items_args = (('item1', 'item2'), ('Return to the previous selection', 'Modify current node'))
        select_item_kwargs = {'header1': 'TEST HEADER MODIFY BOOKMARK',
                              'header2': 'Select a node in the folder <roots> or a command.'}
        # mock methods
        self.pres.view = mock.MagicMock(name='view', spec=View)  # mock View interface
        self.pres.model = mock.MagicMock(name='model', spec=Model)  # mock Model interface
        self.pres.view.main_header = "TEST HEADER MODIFY BOOKMARK"  # set a mocking method header
        # get_children params
        self.pres.model.get_children.side_effect = [get_children_folder, e.NodeNotExists('forced return')]  # 2 calls
        # selected_item params
        self.pres.view.select_item.return_value = True, 0  # folder selection

        result = self.pres.modify_bookmark()  # test of the method

        assert self.pres.model.get_children.call_args.args == ('item1',)
        assert self.pres.view.select_item.call_args.args == select_items_args
        assert self.pres.view.select_item.call_args.kwargs == select_item_kwargs
        assert self.pres.view.output_string.call_args.args == \
               (f'Node <forced return> does not exist {chr(10)}',)
        assert result is False

        # modify current folder case NodeNotExists exception at get_node
        # ---- common params ----
        node_name = 'roots'  # correct name of a new url
        parent_node = None
        get_children_none = False, ()
        get_children_folder = True, ('item1', 'item2')
        selected_field = True, 'name'  # 'name' field selected
        attr_dict = {'name': node_name, 'parent_name': parent_node}  # args to add node
        filtered_attrs = {'name': node_name}  # args to add node

        # mock methods
        self.pres.view = mock.MagicMock(name='view', spec=View)  # mock View interface
        self.pres.model = mock.MagicMock(name='model', spec=Model)  # mock Model interface
        self.pres.view.main_header = "TEST HEADER MODIFY BOOKMARK"  # set a mocking method header
        # get_children params
        self.pres.model.get_children.side_effect = [get_children_folder, e.NodeNotExists('forced return')]  # 2 calls
        # selected_item params
        self.pres.view.select_item.return_value = (False, 1)  # modify current folder
        # get_node params
        self.pres.model.get_node.side_effect = e.NodeNotExists('not exist')

        # switch next traceback output to stderr
        temp = sys.stdout
        sys.stdout = sys.stderr
        print()
        try:
            result = self.pres.modify_bookmark()  # test of the method
        except SystemExit as exc:
            assert type(exc) == SystemExit
            assert exc.code == 1
        sys.stdout = temp  # restore stdout

        # modify current folder, case return to parent folder, node_name = 'roots'
        # ---- common params ----
        node_name = 'roots'  # correct name of a new url
        parent_node = None
        get_children_none = False, ()
        get_children_folder = True, ('item1', 'item2')
        select_items_args = (('item1', 'item2'), ('Return to the previous selection', 'Modify current node'))
        select_item_kwargs = {'header1': 'TEST HEADER MODIFY BOOKMARK',
                              'header2': 'Select a node in the folder <roots> or a command.'}
        # mock methods
        self.pres.view = mock.MagicMock(name='view', spec=View)  # mock View interface
        self.pres.model = mock.MagicMock(name='model', spec=Model)  # mock Model interface
        self.pres.view.main_header = "TEST HEADER MODIFY BOOKMARK"  # set a mocking method header
        # get_children params
        self.pres.model.get_children.return_value = get_children_folder  # 1 call
        # selected_item params
        self.pres.view.select_item.return_value = False, 0  # return to the previous folder

        result = self.pres.modify_bookmark()  # test of the method

        assert self.pres.model.get_children.call_args.args == (node_name,)
        assert self.pres.view.select_item.call_args.args == select_items_args
        assert self.pres.view.select_item.call_args.kwargs == select_item_kwargs
        assert result is True

        # modify current folder, case return to parent folder, node_name != 'roots'
        # can not test this branch

        # modify current folder, case EOF break at select_item
        # ---- common params ----
        node_name = 'roots'  # correct name of a new url
        parent_node = None
        get_children_none = False, ()
        get_children_folder = True, ('item1', 'item2')
        select_items_args = (('item1', 'item2'), ('Return to the previous selection', 'Modify current node'))
        select_item_kwargs = {'header1': 'TEST HEADER MODIFY BOOKMARK',
                              'header2': 'Select a node in the folder <roots> or a command.'}
        # mock methods
        self.pres.view = mock.MagicMock(name='view', spec=View)  # mock View interface
        self.pres.model = mock.MagicMock(name='model', spec=Model)  # mock Model interface
        self.pres.view.main_header = "TEST HEADER MODIFY BOOKMARK"  # set a mocking method header
        # get_children params
        self.pres.model.get_children.return_value = get_children_folder  # 1 call
        # selected_item params
        self.pres.view.select_item.return_value = None, ''  # EOF break

        result = self.pres.modify_bookmark()  # test of the method

        assert self.pres.model.get_children.call_args.args == (node_name,)
        assert self.pres.view.select_item.call_args.args == select_items_args
        assert self.pres.view.select_item.call_args.kwargs == select_item_kwargs
        assert result is False

        # ---- end of the tests ----

    def test_delete_bookmark(self):
        # ---- common params ----
        # delete node successfully
        node_name = 'deleting node'  # correct name of the node
        # mock methods
        self.pres.view = mock.MagicMock(name='view', spec=View)  # mock View interface
        self.pres.model = mock.MagicMock(name='model', spec=Model)  # mock Model interface
        self.pres.view.main_header = "TEST HEADER ADD BOOKMARK"  # set a mocking method header
        # input_line params
        self.pres.view.input_line.return_value = node_name  # 1 call for this case
        prompt_1 = "Input the name of the bookmark", VALID_CHARS

        result = self.pres.delete_bookmark()  # test of the method

        assert self.pres.view.output_header.call_args.args == (self.pres.view.main_header,)  # header output
        assert self.pres.view.input_line.call_args.args == prompt_1  # input_line args
        assert self.pres.model.delete_node.call_count == 1
        assert self.pres.model.delete_node.call_args.args == (node_name, )
        assert self.pres.view.output_string.call_args.args == \
                             (f'Bookmark {node_name} has been deleted {chr(10)}',)
        assert result is True

        # delete node name is None
        node_name = None  # EOF break

        # mock methods
        self.pres.view = mock.MagicMock(name='view', spec=View)  # mock View interface
        self.pres.model = mock.MagicMock(name='model', spec=Model)  # mock Model interface
        self.pres.view.main_header = "TEST HEADER ADD BOOKMARK"  # set a mocking method header
        # input_line params
        self.pres.view.input_line.return_value = node_name  # 1 call for this case
        prompt_1 = "Input the name of the bookmark", VALID_CHARS

        result = self.pres.delete_bookmark()  # test of the method

        assert self.pres.view.output_header.call_args.args == (self.pres.view.main_header,)  # header output
        assert self.pres.view.input_line.call_args.args == prompt_1  # input_line args
        assert result is False

        # delete node name is invalid chars
        node_name = ''  # invalid chars were untered

        # mock methods
        self.pres.view = mock.MagicMock(name='view', spec=View)  # mock View interface
        self.pres.model = mock.MagicMock(name='model', spec=Model)  # mock Model interface
        self.pres.view.main_header = "TEST HEADER ADD BOOKMARK"  # set a mocking method header
        # input_line params
        self.pres.view.input_line.return_value = node_name  # 1 call for this case
        prompt_1 = "Input the name of the bookmark", VALID_CHARS

        result = self.pres.delete_bookmark()  # test of the method

        assert self.pres.view.output_header.call_args.args == (self.pres.view.main_header,)  # header output
        assert self.pres.view.input_line.call_args.args == prompt_1  # input_line args
        assert result is False

        # deleting a node = 'roots' is prohibited
        node_name = 'roots'  # correct name of the node
        # mock methods
        self.pres.view = mock.MagicMock(name='view', spec=View)  # mock View interface
        self.pres.model = mock.MagicMock(name='model', spec=Model)  # mock Model interface
        self.pres.view.main_header = "TEST HEADER ADD BOOKMARK"  # set a mocking method header
        # input_line params
        self.pres.view.input_line.return_value = node_name  # 1 call for this case
        prompt_1 = "Input the name of the bookmark", VALID_CHARS

        result = self.pres.delete_bookmark()  # test of the method

        assert self.pres.view.output_header.call_args.args == (self.pres.view.main_header,)  # header output
        assert self.pres.view.input_line.call_args.args == prompt_1  # input_line args
        # assert self.pres.model.delete_node.call_count == 1
        # assert self.pres.model.delete_node.call_args.args == (node_name,)
        assert self.pres.view.output_string.call_args.args == \
               (f'Folder <{node_name}> can not be deleted {chr(10)}',)
        assert result is False

        # delete node NodeNotExists error
        node_name = 'not exist'  # correct name of the node
        # mock methods
        self.pres.view = mock.MagicMock(name='view', spec=View)  # mock View interface
        self.pres.model = mock.MagicMock(name='model', spec=Model)  # mock Model interface
        self.pres.view.main_header = "TEST HEADER ADD BOOKMARK"  # set a mocking method header
        # input_line params
        self.pres.view.input_line.return_value = node_name  # 1 call for this case
        prompt_1 = "Input the name of the bookmark", VALID_CHARS
        # delete_node params
        self.pres.model.delete_node.side_effect = e.NodeNotExists(node_name)

        result = self.pres.delete_bookmark()  # test of the method

        assert self.pres.view.output_header.call_args.args == (self.pres.view.main_header,)  # header output
        assert self.pres.view.input_line.call_args.args == prompt_1  # input_line args
        assert self.pres.model.delete_node.call_count == 1
        assert self.pres.model.delete_node.call_args.args == (node_name,)
        assert self.pres.view.output_string.call_args.args == \
               (f'Node <{node_name}> does not exist {chr(10)}',)
        assert result is False

        # delete node FolderNotEmpty error
        node_name = 'not exist'  # correct name of the node
        # mock methods
        self.pres.view = mock.MagicMock(name='view', spec=View)  # mock View interface
        self.pres.model = mock.MagicMock(name='model', spec=Model)  # mock Model interface
        self.pres.view.main_header = "TEST HEADER ADD BOOKMARK"  # set a mocking method header
        # input_line params
        self.pres.view.input_line.return_value = node_name  # 1 call for this case
        prompt_1 = "Input the name of the bookmark", VALID_CHARS
        # delete_node params
        self.pres.model.delete_node.side_effect = e.FolderNotEmpty(node_name)

        result = self.pres.delete_bookmark()  # test of the method

        assert self.pres.view.output_header.call_args.args == (self.pres.view.main_header,)  # header output
        assert self.pres.view.input_line.call_args.args == prompt_1  # input_line args
        assert self.pres.model.delete_node.call_count == 1
        assert self.pres.model.delete_node.call_args.args == (node_name,)
        assert self.pres.view.output_string.call_args.args == \
               (f'Folder <{node_name}> is not empty and can not be deleted {chr(10)}',)
        assert result is False

    def test_print_tree(self):
        # common params
        init_node = 'roots'
        init_tab = 0

        # mock methods
        self.pres.view = mock.MagicMock(name='view', spec=View)  # mock View interface
        self.pres.model = mock.MagicMock(name='model', spec=Model)  # mock Model interface
        self.pres.view.main_header = "TEST HEADER PRINT TREE"  # set a mocking method header
        get_children_folder = True, ('FOLDER', )
        get_children_url = False, ('', )

        out_h1 = (self.pres.view.main_header, )
        out_h2 = ('Folder <roots> BEGIN', 0)
        out_h3 = ('Folder <roots> END', 0)
        # get_children params
        self.pres.model.get_children.side_effect = [get_children_folder, get_children_url]  # 2 calls


        result = self.pres.print_tree()  # call the method

        assert self.pres.view.output_header.call_args_list == \
               [(out_h1, ), (out_h2, ), (out_h3, )]
        assert self.pres.model.get_children.call_args_list == \
               [(('roots', ),), (('FOLDER', ), )]
        assert self.pres.view.output_list.call_args.args == (('FOLDER', ), 8)
        assert result is True
