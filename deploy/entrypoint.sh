#!/bin/bash
# Container entrypoint for Automation worker
# Sets timezone and executes passed arguments

export TZ="${TZ:-Asia/Shanghai}"

exec "$@"
