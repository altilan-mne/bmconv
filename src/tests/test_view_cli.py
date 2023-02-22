"""Tests for a bookmark view for CLI UI, module view_cli.py"""

from common import Field
from common import FILL_HEADER, VALID_CHARS

from view_cli import ViewCLI


class TestViewCLI:
    """Testing class for CLI UI, ViewCLI"""
    cli = ViewCLI()

    def test_init_class(self):
        """Test of class initialisation"""
        assert self.cli.view_name == 'cli'
        assert self.cli.main_header == ''
        assert self.cli.local_header == ''
        assert self.cli.node_name == ''

    def test_output_string(self, capsys):
        """Test of the string output"""
        text = 'test string output'
        self.cli.output_string(text)  # output
        captured = capsys.readouterr()  # get the captured stdout
        assert captured.out == text + '\n'

    def test_output_header(self, capsys):
        """Test of the header output"""
        header = 'test header output'
        tab = 8  # value of an indent
        self.cli.output_header(header, tab)  # output
        expected = f"{' ' * 8}{FILL_HEADER * 8} {header} {FILL_HEADER * 8}"
        captured = capsys.readouterr()  # get the captured stdout
        assert captured.out == expected + '\n'

    def test_output_list(self, capsys):
        """Test of the list output."""
        item_list = ('first line', 'second line', 'third line')
        tab = 8  # value of an indent
        self.cli.output_list(item_list, tab)  # output
        expected = (f"{' ' * 8}first line{chr(10)}"
                    f"{' ' * 8}second line{chr(10)}"
                    f"{' ' * 8}third line{chr(10)}")  # chr(10) instead '\n'
        captured = capsys.readouterr()  # get the captured stdout
        assert captured.out == expected

    def test_input_yes_or_no_input(self, monkeypatch):
        """Test of the input yes or no. Input"""
        monkeypatch.setattr('builtins.input', lambda _: "YES")
        assert self.cli.input_yes_or_no('') is True
        monkeypatch.setattr('builtins.input', lambda _: "Foo")
        assert self.cli.input_yes_or_no('') is False

    # def test_input_yes_or_no_output(self, capsys):
    #     """Test of the input yes or no. Prompt output"""
    # capsys returns '' for output of input command prompt ???

    def test_input_line(self, monkeypatch, capsys):
        """Test of the input line."""
        # unfiltered input
        entered = 'Non-filtered text!'
        monkeypatch.setattr('builtins.input', lambda _: entered)
        assert self.cli.input_line('') == entered

        # break entered
        entered = '\x04'
        monkeypatch.setattr('builtins.input', lambda _: entered)
        assert self.cli.input_line('') is None

        # filtered input with invalid chars
        entered = VALID_CHARS + '!'
        monkeypatch.setattr('builtins.input', lambda _: entered)
        assert self.cli.input_line('', VALID_CHARS) == ''  # empty return if invalid chars are occurred
        captured = capsys.readouterr()  # get the captured stdout
        expected = ('Character(s) "!" is/are not allowed in the names\n'
                    'Use alphabetic, numeric and _-. / characters only\n'
                    '\n')
        assert captured.out == expected

    def test_select_line(self, monkeypatch, capsys):
        """Test of the line selection."""
        # main test
        item_list = ('first line', 'second line', 'third line')
        entered = '1'
        monkeypatch.setattr('builtins.input', lambda _: entered)
        assert self.cli.select_line(item_list) == int(entered) - 1
        expected = ('Choose, please, an option or enter EOF and RETURN for input break:\n'
                    '1. first line\n'
                    '2. second line\n'
                    '3. third line\n')
        captured = capsys.readouterr()  # get the captured stdout
        assert captured.out == expected

        # break entered
        entered = '\x04'
        monkeypatch.setattr('builtins.input', lambda _: entered)
        assert self.cli.select_line(item_list) is None
        captured = capsys.readouterr()  # get the captured stdout
        assert captured.out == expected

    def test_select_item(self, monkeypatch, capsys):
        """Test of the item selection."""
        item_list = ('first line', 'second line', 'third line')
        comm_list = ('Command 1', 'Command 2')
        header1 = 'Header1 testing message'
        header2 = 'Header2 testing message'
        expected = ('Header1 testing message\n'
                    'Header2 testing message\n'
                    'Choose, please, an option or enter EOF and RETURN for input break:\n'
                    '1. Command 1\n'
                    '2. Command 2\n'
                    '3. first line\n'
                    '4. second line\n'
                    '5. third line\n')

        # select 2nd line within item_list
        entered = '4'  # 2nd dataline has 4. position, see expected
        monkeypatch.setattr('builtins.input', lambda _: entered)
        result, number = self.cli.select_item(item_list, comm_list=comm_list,
                                              header1=header1, header2=header2)
        assert result is True
        assert number == 1  # index of the 2nd line in item_list
        captured = capsys.readouterr()  # get the captured stdout
        assert captured.out == expected

        # select Command 2 within a command list
        entered = '2'  # 2nd command line has 2. position, see expected
        monkeypatch.setattr('builtins.input', lambda _: entered)
        result, number = self.cli.select_item(item_list, comm_list=comm_list,
                                              header1=header1, header2=header2)
        assert result is False
        assert number == 1  # index of the 2nd line in command list
        captured = capsys.readouterr()  # get the captured stdout
        assert captured.out == expected

        # break entered
        entered = '\x04'
        monkeypatch.setattr('builtins.input', lambda _: entered)
        result, number = self.cli.select_item(item_list, comm_list=comm_list,
                                              header1=header1, header2=header2)
        assert result is None
        captured = capsys.readouterr()  # get the captured stdout
        assert captured.out == expected

    def test_select_field(self, monkeypatch, capsys):
        """Test of the none field selection."""
        filtered_attrs = {'name': 'Bookmark', 'url': 'www.google.com',
                          'icon': 'symbols', 'keywords': 'test Protocol'}
        node_name = 'Data references'
        expected = ('Header1 testing message\n'
                    'Select a field of the node <Data references>\n'
                    'Choose, please, an option or enter EOF and RETURN for input break:\n'
                    '1. Return to the node selection\n'
                    '2. name-> Bookmark\n'
                    '3. url-> www.google.com\n'
                    '4. icon-> symbols\n'
                    '5. keywords-> test Protocol\n')

        # select the field <url>, 3. position
        entered = '3'  # field url has 2. position, see expected
        monkeypatch.setattr('builtins.input', lambda _: entered)
        result, field_name = self.cli.select_field(filtered_attrs, node_name=node_name)
        assert result is True  # data
        assert field_name == 'url'  # selected node name
        captured = capsys.readouterr()  # get the captured stdout
        assert captured.out == expected

        # select Return to the edit node, Command 1
        entered = '1'  # Command Return has 1. position, see expected
        monkeypatch.setattr('builtins.input', lambda _: entered)
        result, number = self.cli.select_field(filtered_attrs, node_name=node_name)
        assert result is False  # command
        assert number == ''  # a command Return to the node selection
        captured = capsys.readouterr()  # get the captured stdout
        assert captured.out == expected

        # break entered
        entered = '\x04'
        monkeypatch.setattr('builtins.input', lambda _: entered)
        result, number = self.cli.select_field(filtered_attrs, node_name=node_name)
        assert result is None
        captured = capsys.readouterr()  # get the captured stdout
        assert captured.out == expected

    def test_edit_field(self, monkeypatch, capsys):
        """Test of the field editing."""

        # edit any fields except the <name> field
        expected = ('-------- Header1 testing message --------\n'
                    'Modify the value of the selected field <keywords>. Use EOF, ENTER for '
                    'cancel\n'
                    'any special chars\n')
        any_field = Field('keywords', 'any special chars')
        entered = '!@#$'  # special chars is allowed
        monkeypatch.setattr('builtins.input', lambda _: entered)
        assert self.cli.edit_field(any_field) == entered
        captured = capsys.readouterr()  # get the captured stdout
        assert captured.out == expected

        # edit the <name> field, filtered input according VALID_CHARS
        expected = ('-------- Header1 testing message --------\n'
                    'Modify the value of the selected field <name>. Use EOF, ENTER for cancel\n'
                    'filtered chars\n')
        any_field = Field('name', 'filtered chars')
        entered = '.-_'  # valid chars
        monkeypatch.setattr('builtins.input', lambda _: entered)
        assert self.cli.edit_field(any_field) == entered
        captured = capsys.readouterr()  # get the captured stdout
        assert captured.out == expected

        # edit the <name> field, filtered input according VALID_CHARS
        # filtering of invalid chars was tested before (see input_line() test)
