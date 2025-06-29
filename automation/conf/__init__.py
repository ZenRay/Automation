#utf:coding:utf8


from __future__ import absolute_import


import logging
from os import path
from configparser import ConfigParser


from .utils import parse_conf
logger = logging.getLogger("automation.conf")

# read maxcomputer configurate file
file = path.join(path.dirname(__file__), "_maxcomputer.ini")
template = """[prod]
access_id: {access_id}
secret_access_key: {secret_acccess_key}
project: {project}
endpoint: {endpoint}
"""
maxcomputer = ConfigParser()
parse_conf(maxcomputer, file, template=template)
logger.info("Load Maxcomputer Configuration Success.")

# read lark configurate file
file = path.join(path.dirname(__file__), "_lark.ini")
template = """[prod]
APP_ID: {APP_ID}
APP_SECRET: {APP_SECRET}
"""

lark = ConfigParser()
parse_conf(lark, file, template=template)
logger.info("Load Lark Configuration Successs.")