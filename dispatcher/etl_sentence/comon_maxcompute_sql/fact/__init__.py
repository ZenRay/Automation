#coding:utf-8

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))


if current_dir not in sys.path:
    sys.path.insert(0, current_dir)
    


from flow import fact_flow_sentence
from trade import fact_trade_sentence