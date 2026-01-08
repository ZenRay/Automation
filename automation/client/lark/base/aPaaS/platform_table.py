#coding:utf-8
""" Lark aPaaS Base Module
* Lark aPaaS Platform Table Base Class
"""

from ._table_field import Field

class TableItem:
    """ Lark aPaaS Platform Table Item Base Class
    """
    def __init__(self, table_name=None, table_desc=None):
        self._table_name = table_name
        self._table_desc = table_desc
        self._fields = []
        
    @property
    def table_name(self):
        return self._table_name
        
    @table_name.setter
    def table_name(self, value):
        self._table_name = value
        
    @property
    def table_desc(self):
        return self._table_desc
        
    @table_desc.setter
    def table_desc(self, value):
        self._table_desc = value
        
    
    @property
    def fields(self):
        return self._fields
    
    
    def add_field(self, field_name, field_type=None, field_desc=None):
        """ Add a field to the table
        
        Args:
        ----------
            field_name: Name of the field
            field_type: Type of the field (optional)
            field_desc: Description of the field (optional)
        """
        field = Field(field_name=field_name, field_type=field_type, field_desc=field_desc)
        self._fields.append(field)
        
        
    @classmethod
    def from_dict(cls, data: dict):
        """ Create TableItem instance from dictionary
        
        Args:
        ----------
            data: dict containing table information
            
        Returns:
        ----------
            TableItem instance
        """
        table_item = cls(
            table_name=data.get("name"),
            table_desc=data.get("description")
        )
        
        for field_data in data.get("columns", []):
            table_item.add_field(
                field_name=field_data.get("name"),
                field_type=field_data.get("data_type"),
                field_desc=field_data.get("description")
            )
        
        return table_item
    
    
    def __str__(self):
        template = f"TableItem(name={self._table_name}, desc={self._table_desc}, fields={{fields}})"
        fields = ", ".join([f"{f.field_name}(type:{f.field_type} desc:{f.field_desc})" for f in self._fields])
        return template.format(fields=fields)
    
    
    