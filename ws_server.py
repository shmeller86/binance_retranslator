from elasticsearch import Elasticsearch
from datetime import datetime, timezone
from pprint import pprint as pp
import asyncio
import random
import websockets
import json
import sys
import threading
from dotenv import load_dotenv
load_dotenv()
es = Elasticsearch()


class Emulator:
    data = None
    ts_from = None
    index = None
    delta = 60

    def __init__(self,symbol, dt):
        self.ts_from = int(datetime.strptime(dt, "%d.%m.%Y %H:%M:%S.%f").replace(tzinfo=timezone.utc).timestamp() * 1000)
        self.index = symbol

        self._getData()

    def _ts_to(self, ts_from):
        return ts_from + self.delta * 1000

    def _getData(self):
        q = {
            "query": {
                "range": {
                    "ts": {
                        "gte": self.ts_from,
                        "lt": self._ts_to(self.ts_from)
                    }
                }
            },
            "size": 1000,
        }
        resp = es.search(index=self.index, body=q)

        if resp['hits']['total']['value'] > 0:
            self.data = [hit['_source'] for hit in resp['hits']['hits']]
        print("Got %d Hits" % resp['hits']['total']['value'])
        print("In self.data: %d"% len(self.data))
    
    def _append(self, ts_from):
        q = {
            "query": {
                "range": {
                    "ts": {
                        "gte": ts_from,
                        "lt": self._ts_to(ts_from)
                    }
                }
            },
            "size": 1000,
        }
        resp = es.search(index=self.index, body=q)

        if resp['hits']['total']['value'] > 0:
            self.data.extend([hit['_source'] for hit in resp['hits']['hits']])
        print("Got %d Hits" % resp['hits']['total']['value'])
        print("In self.data after append: %d"% len(self.data))

    async def translation(self, websocket, path):
        while True:
            if self.data:
                current = self.data.pop(0)
                sys.stdout.write("\rCurrent length: %d "% len(self.data))    
                sys.stdout.flush()
                await websocket.send(json.dumps(current))
                if len(self.data) < 500:
                    threading.Thread(target=self._append, args=[self.data[len(self.data)-1]["ts"]]).start()

            await asyncio.sleep(int(current['delay'])/1000)
    
    def run(self):
        start_server = websockets.serve(self.translation, '127.0.0.1', 5678)
        asyncio.get_event_loop().run_until_complete(start_server)
        asyncio.get_event_loop().run_forever()


em = Emulator("atomusdt","01.01.2022 10:29:20.221")
em.run()



