"""Implementation of View  for simple CLI console."""

from common import FILL_HEADER, VALID_CHARS  # import constants
from common import Field  # import a namedtuple Field
import typing as t


class ViewCLI:
    """Class of View implementation for CLI console.
    Presents a swallow user interface.

    :param: No Parameters
    """
    view_name = 'cli'  #: the name for all cli views


    def __init__(self):
        """Constructor method.
        """
        self.main_header: str = ''  #: main header of view
        self.local_header: str = ''  #: local header of view
        self.node_name: str = ''  #: node name of the view instance

    # ---- output section ----
    @staticmethod
    def output_string(text: str):
        """Output a text.

        :param text: a text to print
        :return: nothing
        """
        print(text)

    @staticmethod
    def output_header(header: str, tab: int = 0):
        """Print a header with tab spaces on the left.
        Fill the header with the FILL_HEADER character

        :param header: text for heading
        :param tab: optional number of tabs for a left indent, defaults to 0
        :return: nothing
        """
        print(' ' * tab, end='')
        print(FILL_HEADER * 8, header, FILL_HEADER * 8)  # print the header of the action

    @staticmethod
    def output_list(item_list: tuple[str, ...], tab: int = 0):
        """Print a list of items with tab spaces on the left.

        :param item_list: item list to print
        :param tab: optional number of tabs for a left indent, defaults to 0
        :return: nothing
        """
        [print(' ' * tab + item) for item in item_list]

    # ---- input section ----
    @staticmethod
    def input_yes_or_no(prompt: str) -> bool:
        """Get user input yes or no.

        :param prompt: prompt text for the user request
        :return: True for yes and False otherwise
        """
        try:
            answer = input(prompt + ' --> ')    # input a string
        except EOFError:  # intercept Ctr-D exception for Linux
            print()
            answer = ''
        if answer.lower() == 'yes':  # convert input to lowercase
            return True
        else:
            return False

    @staticmethod
    def input_line(prompt: str, valid_chars: str = '') -> t.Optional[str]:
        """Get user input string. Filter the user input out of non-alphabetical and non-numeral
        characters, allow additional characters from the user template, optionally.

        :param prompt: prompt text for the user request
        :param valid_chars: template of valid characters, default to empty
        :return: entered string, empty string if non-allowed characters were occurred, None for EOF break
        """
        try:
            text = input(prompt + ' --> ')  # input a string
        except EOFError:  # intercept Ctr-D exception for Linux
            text = '\04'
        if '\x04' in text:  # EOF was entered
            return None  # break
        wrong_chars = ''  # a string for incorrect chars
        if valid_chars:
            for x in text:
                if not x.isalnum() and x not in valid_chars:  # check if a char is NOT acceptable
                    wrong_chars = wrong_chars + x  # store the wrong char
            if wrong_chars:
                print(f'Character(s) "{wrong_chars}" is/are not allowed in the names')
                print(f"Use alphabetic, numeric and {valid_chars} characters only{chr(10)}")
                return ''  # text contains invalid chars, return empty string
        return text

    @staticmethod
    def select_line(item_list: tuple[str, ...]) -> t.Optional[int]:
        """Get a custom row selection from a list of rows. Submit a list
        in the form of a menu with numbered lines from 1 to the length of the list.

        :param item_list: list of rows to selection
        :return: number of the selected row or None for EOF break
        """
        while True:
            print("Choose, please, an option or enter EOF and RETURN for input break:")  # user prompt
            items = [print(str(i+1) + '. ' + item) for i, item in enumerate(item_list)]  # get numbered list
            try:
                item_number = input("--> ")  # get the input
            except EOFError:  # intercept Ctr-D exception for Linux
                item_number = '\04'
            # check if the input is an eligible number, if not - repeat input
            if item_number.isdigit() and int(item_number) in range(1, len(items) + 1):
                return int(item_number) - 1  # return row index in the list
            else:
                if item_number == '\x04':
                    return None  # break
                print(f"Input, please, a number from 1 to {len(items)}{chr(10)}")  # error message

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
        # add commands to the selection list
        if comm_list:
            lines = comm_list + item_list  # make a final list to select
        else:
            lines = item_list  # keep the original list
        if header1:
            self.main_header = header1  # set main header
        if header2:
            self.local_header = header2  # set local header

        print(self.main_header)  # print main header
        print(self.local_header)  # print local header

        selected_no = self.select_line(lines)  # make a selection

        if selected_no is None:
            return None, 0  # break
        elif selected_no < len(comm_list):
            return False, selected_no  # return a command index
        else:
            return True, selected_no - len(comm_list)  # return an item index

    def select_field(self, filtered_attrs: dict[str, str], node_name: str = '') -> tuple[t.Optional[bool], str]:
        """Select a bookmark node field to edit.

        :param filtered_attrs: ordered dictionary of the node fields to select
        :param node_name: name of the bookmark node whose field is going to be edited, default to empty
        :return: a tuple (result, index), that means:
                (None, '') - no selection
                (False, '') - the command 'Return to the node selection'
                (True, field name) - selected field name
        """
        if node_name:
            self.node_name = node_name  # set node_name
        self.local_header = f'Select a field of the node <{node_name}>'  # set local header
        lines = tuple([f"{key}-> {value}" for key, value in filtered_attrs.items()])  # a list to select
        comm_list = ('Return to the node selection', )  # add one command to selection list
        result, index = self.select_item(lines, comm_list=comm_list, header2=self.local_header)  # select a field
        if result is None:
            return None, ''  # no selection
        elif result:
            return True, tuple(filtered_attrs.keys())[index]  # return the name of the selected field
        else:
            return False, ''  # the command 'Return to the node selection'

    def edit_field(self, field: Field) -> t.Optional[str]:
        """Edit a field of a bookmark node. If the field type is <name>,
        then the valid characters for the field value are only
        alphabetic, numeric, and additional characters from VALID_CHARS.

        :param field: tuple Field that contains the field name and the field value
        :return: new field value or None if EOF - break
        """
        if field.name == 'name':  # editing field is 'name'
            valid_chars = VALID_CHARS  # enable keycode checking if it is within str.isalnum and VALID_CHARS filters
        else:
            valid_chars = ''  # no keycode checking

        self.local_header = (f"Modify the value of the selected field <{field.name}>. "
                             f"Use EOF, ENTER for cancel")  # local header

        while True:
            self.output_header(self.main_header)   # print a main header
            print(self.local_header)  # print a local header
            self.output_string(field.text)  # print current text of the field

            # input a line
            new_text = self.input_line(prompt="", valid_chars=valid_chars)
            if new_text is None or new_text:  # check input
                break  # invalid chars is occurred, repeat the input
        return new_text  # return entered text or None if break


def main():
    """Show main options of the class ViewCLI

    :return:
    """
    print()
    print()
    cli = ViewCLI()
    cli.output_header(f'Show the functionality of {type(cli).__name__}')
    item_list = ('first line', 'second line', 'third line')
    cli.output_header('Show output_list()')
    cli.output_list(item_list, 8)
    print()

    cli.output_header('Show input_line()')
    print('Show input_line() with filtering')
    new_line = cli.input_line('Input string with invalid chars like !@#$:', VALID_CHARS)
    print(new_line)
    print()

    print('Show input_line() without filtering')
    new_line = cli.input_line('Input string with invalid chars like !@#$:')
    print(new_line)
    print()

    cli.output_header('Show select_item()')
    header1 = 'Header1 testing message'
    header2 = 'Header2 testing message'
    comm_list = ('Return to the previous selection', )

    result, number = cli.select_item(item_list, comm_list=comm_list, header1=header1, header2=header2)
    if result:
        print(result, number, item_list[number])  # data was selected
    elif result is None:
        print('No selection')  # break was selected
    else:
        print('Return to the previous selection')  # command 0 was selected
    print()

    cli.output_header('Show select_field()')
    filtered_attrs = {'name': 'Bookmark', 'url': 'www.google.com', 'icon': 'symbols', 'keywords': 'test Protocol'}
    result, field_name = cli.select_field(filtered_attrs, node_name='Google data')
    if result:
        print(result, field_name)  # data was selected
    elif result is None:
        print('No selection')  # break was selected
    else:
        print('Return to the previous selection')  # command 0 was selected
    print()

    cli.output_header('Show edit!@#$_field()')
    edited_field = Field('name', 'for name use the valid chars only')
    new_field = cli.edit_field(edited_field)
    if new_field is None:
        print('No changes')
    else:
        print(new_field)

if __name__ == '__main__':
    main()
