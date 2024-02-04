#utf:coding:utf8


from __future__ import absolute_import


import logging
from os import path
from configparser import ConfigParser

logger = logging.getLogger("automation.conf")

# read maxcomputer configuer file
file = path.join(path.dirname(__file__), "_maxcomputer.ini")
template = """[prod]
access_id: {access_id}
secret_access_key: {secret_acccess_key}
project: {project}
endpoint: {endpoint}
"""
maxcomputer = ConfigParser()
if path.exists(file):
    maxcomputer.read(file)
else:
    with open(file, "w", encoding="utf8") as writer:
        writer.write(template)
    maxcomputer.read(file)

del file, template
logger.info("Load Maxcomputer Configuration Success")