#codint:utf8

import logging


from automation.conf import maxcomputer
from automation.client import MaxComputerClient
conf = {
    "access_id": maxcomputer.get("dev", "access_id"),
    "secret_access_key": maxcomputer.get("dev", "secret_access_key"),
    "project": maxcomputer.get("dev", "project"),
    "endpoint":maxcomputer.get("dev", "endpoint")
}


logger = logging.getLogger("automation")
logger.setLevel(logging.DEBUG)

formater = logging.Formatter(
    fmt="[%(levelname)s]:%(asctime)s %(message)s",datefmt="%Y-%m-%d %H:%M:%S"
)
handler = logging.StreamHandler()
handler.setFormatter(formater)
logger.addHandler(handler)



client = MaxComputerClient(**conf)

hints = {
    "odps.sql.allow.fullscan": True,
    "odps.sql.type.system.odps2": True,
    "odps.sql.decimal.odps2": True,
    "odps.sql.hive.compatible": True,
    "odps.odtimizer.dynamic.filter.dpp.enable": True,
    "odps.odtimizer.enable.dynamic.filter": True,
    "odps.sql.python.version": "cp37",
}