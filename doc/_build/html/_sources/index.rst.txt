.. Bookmark manager (a learning project) documentation master file, created by
   sphinx-quickstart on Tue Mar  7 13:50:24 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Bookmark manager's documentation!
=================================================================

Description
-----------
A bookmark editor created and refactored during the process of Python studying.
Editor creates a tree of nodes, each node is a folder or URL.
User can create a database from scratch or make a copy of Google Chrome bookmarks from Bookmarks.json file.
MVP pattern was implemented. Interface protocols were added to separate the Presenter and View, Model parts.
In the future it supposes to add other implementations of View with quasi-graphics and GUI, and SQL for the Model part.

Notes
-----
- Release 1.0 has an own internal database structure and keep it an JSON file.
- User interface was implemented by CLI.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   modules



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
