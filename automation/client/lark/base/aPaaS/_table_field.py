#coding:utf-8
"""Lark aPaaS Table Field Base Module
"""

from collections import namedtuple

Field = namedtuple(
    'Field', ['field_name', 'field_type', 'field_desc'], defaults=[None, None]
)