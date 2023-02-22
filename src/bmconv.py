"""File convertor for Bookmark manager project.
Convert the format of storage files as JSON, SOLite and more.
Use the command line for the user interface.
Implemented with the built-in argparse library.

"""

import argparse
from functools import partial
import json


# common data
version = 'BM file converter 2.0'
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

class JsonTree:
    """A class of a internal bookmark structure using JSON format to store data.
    """



# ---- section of format conversions
def _chrome_into_object(self, dt: dict) -> dict:
    """Conversation of chrome json structure to the internal node tree.
    Inner recursion for convert_chrome method, in-place conversion

    :param dt: a dictionary with chrome json structure
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


def convert_chrome(self, filename: str) -> tuple[bool, str]:
    """Convert Chrome bookmark JSON filename to the current tree. Return (True/False, error message)

    :param filename: Google bookmark filename to convert
    :return: (True, empty string)  or (False, error message)
    """
    # ---- open and load JSON file of Chrome bookmarks, the file exists
    with open(filename, 'r', encoding='utf-8') as f:  # open the tree image file, or raise FileNotFoundError
        source_file = json.load(f)  # read the json image and then close the file, image is a dict

    # ---- extract the roots dict only, checksum, sync_metadata and version attrs do not use
    chrome_keys = list(source_file)  # get chrome main keys of the bookmark object
    if 'checksum' and 'roots' and 'version' not in chrome_keys:  # if keys don't exist that is wrong file format
        return False, f'Source file {filename} has wrong format'

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

    self._save_tree()  # save the updated current root

    return True, ''


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
ap.add_argument('-f', '--from', choices=list_input_formats,
                help='source format', required=True)
ap.add_argument('-t', '--to', choices=list_output_formats,
                help='destination format', required=True)
ap.add_argument('source')
ap.add_argument('destination')
args = ap.parse_args()


print(args)

if __name__ == '__main__':
    main()













