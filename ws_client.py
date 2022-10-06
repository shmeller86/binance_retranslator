import asyncio
import websockets
from pprint import pprint as pp
import json
import os
import sys
from collections import OrderedDict
from dotenv import load_dotenv
load_dotenv()

class WSClient:
    asks = dict()
    bids = dict()
    best_bid = None
    best_ask = None

    async def connect(self):
        uri = os.environ['WS_URL']
        async with websockets.connect(uri) as websocket:
            while 1:
                obj = await websocket.recv()
                obj = json.loads(obj)
                
                best_bid = .0
                for n in obj['b']: 
                    if best_bid < n[0] and n[1] > .0:
                        best_bid = n[0]
                self.best_bid = best_bid

                best_ask = 999999999
                for n in obj['a']: 
                    if best_ask > n[0] and n[1] > .0:
                        best_ask = n[0]
                self.best_ask = best_ask

                self._pp()

    def _pp(self):
        sys.stdout.write("\rASK: %f BID: %f SPREAD: %f\t\t\t"% (self.best_ask,self.best_bid, (self.best_ask-self.best_bid)))
        sys.stdout.flush()

try:
    ws = WSClient()
    asyncio.run(ws.connect())
except KeyboardInterrupt:
    print("\n Stop")