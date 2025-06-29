#coding:utf8
"""Configuration Parse Utils"""
from os import path


def parse_conf(parser, file, template):
    if not path.exists(file):
        with open(file, "w", encoding="utf8") as writer:
            writer.write(template)
    
    parser.read(file)