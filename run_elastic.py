import datetime
import os
import sys
from pprint import pprint as pp
from memory_profiler import profile
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk, bulk
from dotenv import load_dotenv
load_dotenv()

class Parser:
    es = Elasticsearch(os.environ["DB"])
    symbol = None
    iterator = 1
    bulk_data = list()

    def __init__(self, symbol):
        self.symbol = symbol

    def add(self, data: list, send: int = 0):
        self.bulk_data.append({
                "_op_type": "index",
                "_index": self.symbol,
                "_source": data,
            })
        if len(self.bulk_data) == 1000 or send == 1:
            bulk(self.es, self.bulk_data)
            self.bulk_data = list()
        self.iterator+=1

    def prepare(self, fl: str):
        with open(fl, "r") as f:
            i = 0; _data = dict({"ts":0,"a":[],"b":[],"delay":0})
            for n in f:
                if i==0: i+=1; continue
                if (i%1000000)==0: print(i)
                nd = n.split(",")
                if int(nd[0]) == _data["ts"]:
                    if nd[1] == 'a':
                        _data['a'].append( [float(nd[2]), float(nd[3])] )
                    elif nd[1] == 'b':
                        _data['b'].append( [float(nd[2]), float(nd[3])] )
                elif not _data["ts"]:
                    if nd[1] == 'a':
                        _data['a'].append( [float(nd[2]), float(nd[3])] )
                    elif nd[1] == 'b':
                        _data['b'].append( [float(nd[2]), float(nd[3])] )
                    _data["ts"] = int(nd[0])
                    _data['delay'] = float(nd[4].replace("\n", ""))
                else:
                    self.add(_data)
                    _data = dict({"ts":0,"a":[],"b":[],"delay":0})
                i+=1
            if _data: self.add(_data,1); del _data

name = os.environ["SYMBOL"]
p = Parser(name)

directory = os.environ["PATH_TO_FILES"]
for n in  os.listdir(directory):
    nd = os.path.join(directory,n)
    if os.path.isdir(nd) and name.upper() in n:
        for f in os.listdir(nd):
            print(nd + "/"+ f)
            s = datetime.datetime.now()
            p.prepare(nd + "/"+ f)
            delay = datetime.datetime.now() - s
            print("Time execute: " + str(delay))
            print("Rows in DB: " + str(p.iterator))