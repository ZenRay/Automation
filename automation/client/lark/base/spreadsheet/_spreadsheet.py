#coding:utf-8
""" Lark Spreadsheet Metadata
"""
import logging

from ...exceptions import LarkSheetException

from ._sheet import SheetItem


logger = logging.getLogger("automation.client.lark.base._spreadsheet")


class SpreadSheetMeta:
    """ Lark Spreadsheet Metadata
    """

    def __init__(self):
        self._spreadsheet_token = None
        self._spreadsheet_title = None
        self._sheet_items = []

    @property
    def spreadsheet_token(self):
        return self._spreadsheet_token
    
    @spreadsheet_token.setter
    def spreadsheet_token(self, value):
        self._spreadsheet_token = value

    @property
    def spreadsheet_title(self):
        return self._spreadsheet_title

    @spreadsheet_title.setter
    def spreadsheet_title(self, value):
        self._spreadsheet_title = value

    @property
    def sheet_items(self):
        if len(self._sheet_items) == 0:
            logger.debug("No sheet items found")
            
        return self._sheet_items

    @sheet_items.setter
    def sheet_items(self, value):
        if isinstance(value, dict):
            self._sheet_items.append(SheetItem(**value))
        elif isinstance(value, SheetItem):
            self._sheet_items.append(value)
        elif isinstance(value, (list, tuple)) and all(isinstance(item, (dict, SheetItem)) for item in value):
            for item in value:
                if isinstance(item, dict):
                    self._sheet_items.append(SheetItem(**item))
                elif isinstance(item, SheetItem):
                    self._sheet_items.append(item)
                else:
                    raise LarkSheetException(f"Invalid sheet item type: {type(item)}. Must be dict or SheetItem.")
        else:
            raise LarkSheetException(f"Invalid sheet items type: {type(value)}. Must be dict, SheetItem, or list/tuple of dict/SheetItem.")
        


    def append(self, item):
        """Add a sheet item"""
        self.sheet_items = item
        
    def clear_sheets_meta(self):
        """Clear all sheet items
        
        Clear Sheets Meta Information
        """
        self._sheet_items = []
        
    def get_sheet_by_id(self, sheet_id):
        """Get sheet item by sheet_id"""
        for sheet in self._sheet_items:
            if sheet.sheet_id == sheet_id:
                return sheet
        return None
        
    def get_sheet_by_title(self, sheet_title):
        """Get sheet item by sheet_title"""
        for sheet in self._sheet_items:
            if sheet.sheet_title == sheet_title:
                return sheet
        return None


    def __repr__(self):
        return f"<SpreadSheetMeta title='{self._spreadsheet_title}' token='{self._spreadsheet_token}' at {hex(id(self))}>"
        
    # List-like behavior
    def __len__(self):
        return len(self._sheet_items)
        
    def __getitem__(self, index_or_title):
        # Return by index if integer is provided
        if isinstance(index_or_title, int):
            return self._sheet_items[index_or_title]
        
        # Find sheet by title if string is provided
        for sheet in self._sheet_items:
            if sheet.sheet_title == index_or_title:
                return sheet
                
        # Raise KeyError if not found
        raise KeyError(f"Sheet with title '{index_or_title}' not found")
        
    def __iter__(self):
        return iter(self._sheet_items)