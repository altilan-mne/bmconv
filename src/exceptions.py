"""All custom error exceptions in the project are here
They inherit from a base class MyProjectError(Exception)
"""


class MyProjectError(Exception):
    """A base class for my project exceptions"""
    pass


class BadEpochType(MyProjectError):
    """A bad type of time epoch'"""
    def __init__(self, epoch_type):
        self.epoch_type = epoch_type
        super().__init__(
            f'Unknown epoch type "{epoch_type}" '
            'at the time conversion'
        )


class NodeNotExists(MyProjectError):
    """Raise if named node doesn't exist in the common dict of nodes. It returns an appropriate error message"""
    def __init__(self, node_name):
        super().__init__(
            f'Node <{node_name}> does not exist {chr(10)}'
        )


class FolderNotExist(MyProjectError):
    """Raise if named folder doesn't exist in the common dict of nodes. It returns an appropriate error message"""
    def __init__(self, folder_name):
        super().__init__(
            f'Node <{folder_name}> is not a folder {chr(10)}'
        )


class FolderNotEmpty(MyProjectError):
    """Raise at a non-empty folder deleting request. It returns an appropriate error message"""
    def __init__(self, folder_name):
        super().__init__(
            f'Folder <{folder_name}> is not empty and can not be deleted {chr(10)}'
        )

