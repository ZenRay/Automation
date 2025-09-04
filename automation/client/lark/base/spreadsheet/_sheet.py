# coding:utf-8
""" Lark Sheet Metadata

"""


from ...exceptions import LarkSheetException


class SheetItem:
    """ Single Sheet Item
    """
    def __init__(self, sheet_id=None, sheet_title=None):
        self._sheet_id = sheet_id
        self._sheet_title = sheet_title
        
    @property
    def sheet_id(self):
        return self._sheet_id
        
    @sheet_id.setter
    def sheet_id(self, value):
        self._sheet_id = value
        
    @property
    def sheet_title(self):
        return self._sheet_title
        
    @sheet_title.setter
    def sheet_title(self, value):
        self._sheet_title = value
        
    def __getitem__(self, key):
        if key == self._sheet_title or key == 'sheet_title':
            return self._sheet_id
        # competitive sheet_id
        elif key == 'sheet_id':
            return self._sheet_id
        else:
            raise KeyError(f"Key '{key}' not found")
            
    def __setitem__(self, key, value):
        if key == self._sheet_title or key == 'sheet_title':
            self._sheet_id = value
        elif key == 'sheet_id':
            self._sheet_id = value
        
        else:
            self._sheet_title = key
            self._sheet_id = value
            
    def __contains__(self, key):
        return key == self._sheet_title or key in ('sheet_id', 'sheet_title')
    
    
    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default
            
    def __repr__(self):
        return f"<Sheet title='{self._sheet_title}' id='{self._sheet_id}' at {hex(id(self))}>"


    def __format__(self, format_spec=''):
        return f"Sheet(title='{self._sheet_title}', id='{self._sheet_id}')"