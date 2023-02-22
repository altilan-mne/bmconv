"""Interfaces between Presenter class and View aimplementations."""

import typing as t
from common import Field  # import a namedtuple Field


class ViewProto(t.Protocol):
    """Prototype class of View."""

    view_name: str  #: name of the View realization
    main_header: str  #: main header of the view
    local_header: str  #: local header of the view
    node_name: str  #: current node name

    # ---- output section ----
    @staticmethod
    def output_string(text: str):
        """Output a text.

        :param text: a text to print
        :return: nothing
        """

    @staticmethod
    def output_header(header: str, tab: int = 0):
        """Print a header with tab spaces on the left.
        Fill the header with the FILL_HEADER character

        :param header: text for heading
        :param tab: optional number of tabs for a left indent, defaults to 0
        :return: nothing
        """

    @staticmethod
    def output_list(item_list: tuple[str, ...], tab: int = 0):
        """Print a list of items with tab spaces on the left.

        :param item_list: item list to print
        :param tab: optional number of tabs for a left indent, defaults to 0
        :return: nothing
        """

    # ---- input section ----
    @staticmethod
    def input_yes_or_no(prompt: str) -> bool:
        """Get user input yes or no.

        :param prompt: prompt text for the user request
        :return: True for yes and False otherwise
        """

    @staticmethod
    def input_line(prompt: str, valid_chars: str = '') -> t.Optional[str]:
        """Get user input string. Filter the user input out of non-alphabetical and non-numeral
        characters, allow additional characters from the user template, optionally.

        :param prompt: prompt text for the user request
        :param valid_chars: template of valid characters, default to empty
        :return: entered string, empty string if non-allowed characters were occurred, None for EOF break
        """

    @staticmethod
    def select_line(item_list: tuple[str, ...]) -> t.Optional[int]:
        """Get a custom row selection from a list of rows. Submit a list
        in the form of a menu with numbered lines from 1 to the length of the list.

        :param item_list: list of rows to selection
        :return: number of the selected row or None for EOF break
        """

    def select_item(self, item_list: tuple[str, ...], comm_list: tuple[str, ...] = (),
                    header1: str = '', header2: str = '') -> tuple[t.Optional[bool], int]:
        """Get a custom item selection from a list of item. Submit a list
        in the form of a menu with numbered lines from 1 to the length of the list.
        Add a list of commands to the selection list, optionally.
        Print two headers to inform the user, optionally

        :param item_list: list of data rows to selection
        :param comm_list: list of command rows to selection, default to empty
        :param header1: header1 text, default to empty
        :param header2: header2 text, default to empty
        :return: a tuple (result, index), that means:
                (None, 0) - no selection
                (False, index) - a command with index number was selected
                (True, index) - a data item with the index number was selected
        """

    def select_field(self, filtered_attrs: dict[str, str], node_name: str = '') -> tuple[t.Optional[bool], str]:
        """Select a bookmark node field to edit.

        :param filtered_attrs: ordered dictionary of the node fields to select
        :param node_name: name of the bookmark node whose field is going to be edited, default to empty
        :return: a tuple (result, index), that means:
                (None, '') - no selection
                (False, '') - the command 'Return to the node selection'
                (True, field name) - selected field name
        """

    def edit_field(self, field: Field) -> t.Optional[str]:
        """Edit a field of a bookmark node. If the field type is <name>,
        then the valid characters for the field value are only
        alphabetic, numeric, and additional characters from VALID_CHARS.

        :param field: tuple Field that contains the field name and the field value
        :return: new field value or None if EOF - break
        """

class View:
    """View class"""

    def __init__(self, proto: ViewProto):
        self.proto = proto
        self.main_header = self.proto.main_header
        self.local_header = self.proto.local_header
        self.node_name = self.proto.node_name

    # ---- output section ----
    def output_string(self, text: str):
        """Output a text.

        :param text: a text to print
        :return: nothing
        """
        self.proto.output_string(text)

    def output_header(self, header: str, tab: int = 0):
        """Print a header with tab spaces on the left.
        Fill the header with the FILL_HEADER character

        :param header: text for heading
        :param tab: optional number of tabs for a left indent, defaults to 0
        :return: nothing
        """
        self.proto.output_header(header, tab)

    def output_list(self, item_list: tuple[str, ...], tab: int = 0):
        """Print a list of items with tab spaces on the left.

        :param item_list: item list to print
        :param tab: optional number of tabs for a left indent, defaults to 0
        :return: nothing
        """
        self.proto.output_list(item_list, tab)

    # ---- input section ----
    def input_yes_or_no(self, prompt: str) -> bool:
        """Get user input yes or no.

        :param prompt: prompt text for the user request
        :return: True for yes and False otherwise
        """
        return self.proto.input_yes_or_no(prompt)

    def input_line(self, prompt: str, valid_chars: str = '') -> t.Optional[str]:
        """Get user input string. Filter the user input out of non-alphabetical and non-numeral
        characters, allow additional characters from the user template, optionally.

        :param prompt: prompt text for the user request
        :param valid_chars: template of valid characters, default to empty
        :return: entered string, empty string if non-allowed characters were occurred, None for EOF break
        """
        return self.proto.input_line(prompt, valid_chars)

    def select_line(self, item_list: tuple[str, ...]) -> t.Optional[int]:
        """Get a custom row selection from a list of rows. Submit a list
        in the form of a menu with numbered lines from 1 to the length of the list.

        :param item_list: list of rows to selection
        :return: number of the selected row or None for EOF break
        """
        return self.proto.select_line(item_list)

    def select_item(self, item_list: tuple[str, ...], comm_list: tuple[str, ...] = (),
                    header1: str = '', header2: str = '') -> tuple[t.Optional[bool], int]:
        """Get a custom item selection from a list of item. Submit a list
        in the form of a menu with numbered lines from 1 to the length of the list.
        Add a list of commands to the selection list, optionally.
        Print two headers to inform the user, optionally

        :param item_list: list of data rows to selection
        :param comm_list: list of command rows to selection, default to empty
        :param header1: header1 text, default to empty
        :param header2: header2 text, default to empty
        :return: a tuple (result, index), that means:
                (None, 0) - no selection
                (False, index) - a command with index number was selected
                (True, index) - a data item with the index number was selected
        """
        return self.proto.select_item(item_list, comm_list, header1, header2)

    def select_field(self, filtered_attrs: dict[str, str], node_name: str = '') -> tuple[t.Optional[bool], str]:
        """Select a bookmark node field to edit.

        :param filtered_attrs: ordered dictionary of the node fields to select
        :param node_name: name of the bookmark node whose field is going to be edited, default to empty
        :return: a tuple (result, index), that means:
                (None, '') - no selection
                (False, '') - the command 'Return to the node selection'
                (True, field name) - selected field name
        """
        return self.proto.select_field(filtered_attrs, node_name)

    def edit_field(self, field: Field) -> t.Optional[str]:
        """Edit a field of a bookmark node. If the field type is <name>,
        then the valid characters for the field value are only
        alphabetic, numeric, and additional characters from VALID_CHARS.

        :param field: tuple Field that contains the field name and the field value
        :return: new field value or None if EOF - break
        """
        return self.proto.edit_field(field)
