from flask import Flask
import ccxt
import time
import redis
from flask_crontab import Crontab
import json
from arctic import Arctic
import sys
import concurrent.futures
import pandas as pd

app = Flask(__name__)
crontab = Crontab(app)
store = Arctic("localhost")
store.initialize_library("BINANCE_TEST")


@crontab.job()
@app.route('/get1m')
def process_1m_data():
    count = 0

    def _fetch_result(symbol):
        data = binance.fetch_ohlcv(symbol, "1m", int((time.time() // 60 - 1) * 60000))
        r = redis.Redis(host='localhost', port=6379, db=0)
        library = store['BINANCE_TEST']
        if len(data) > 0:
            if not r.exists(str(int((time.time() // 60) * 60000)) + symbol + "-1m"):
                df = pd.DataFrame([data[0]], columns=['t', 'o', 'h', 'l', 'c', 'v'])
                r.set(str(int((time.time() // 60) * 60000)) + symbol + "-1m", str(json.dumps(data[0])))
                library.append(symbol + "-1m", df)
            return data[0]

        return []

    try:
        r = redis.Redis(host='localhost', port=6379, db=0)
        binance = ccxt.binance()
        start = time.time()
        symbols = json.loads(r.get("symbols"))
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(symbols)) as executor:
            market_workers = {executor.submit(_fetch_result, symbol):
                                  symbol for symbol in symbols}
        end = time.time()
        print(f'{end - start:.2f}')
        print("Success")
    except Exception as e:
        print(e)
    return str(count)


# open -1
# high -2
# low -3
# close -4
# volume-5
@crontab.job(minute="*/5")
@app.route('/get5m')
def process_5m_data():
    time.sleep(30)
    start = time.time()
    r = redis.Redis(host='localhost', port=6379, db=0)
    library = store['BINANCE_TEST']
    symbols = json.loads(r.get("symbols"))
    for symbol in symbols:
        volume = 0.0
        high = -sys.maxsize
        low = sys.maxsize
        open = 0.0
        close = 0.0
        for i in range(0, 5):
            time_epochs = int((time.time() // 60 - i) * 60000)
            key = (str(time_epochs) + symbol + "-1m")

            if r.exists(key):
                li = r.get(key)
                row = json.loads(li)
                volume = volume + row[5]
                if i == 0:
                    close = row[4]
                if i == 4:
                    open = row[1]
                high = max(high, row[2])
                low = min(low, row[3])
                r.delete(key)
        output_list = [int((time.time() // 60) * 60000), open, high, low, close, volume]
        if not r.exists(str(int((time.time() // 60) * 60000)) + symbol + "-5m"):
            r.set(str(int((time.time() // 60) * 60000)) + symbol + "-5m", str(json.dumps(output_list)))
            df = pd.DataFrame([output_list], columns=['t', 'o', 'h', 'l', 'c', 'v'])
            library.write(symbol + '-5m', df)

    end = time.time()
    print(f'{end - start:.2f}')
    return "5m Job executed successfully"


@crontab.job(minute="*/15")
@app.route('/get15m')
def process_15m_data():
    time.sleep(35)
    r = redis.Redis(host='localhost', port=6379, db=0)
    library = store['BINANCE_TEST']
    symbols = json.loads(r.get("symbols"))
    for symbol in symbols:
        volume = 0.0
        high = -sys.maxsize
        low = sys.maxsize
        open = 0.0
        close = 0.0
        for i in range(0, 3):
            time_epochs = int((time.time() // 60 - i * 5) * 60000)
            key = str(time_epochs) + symbol + "-5m"
            if r.exists(key):
                li = r.get(key)
                row = json.loads(li)
                volume = volume + row[5]
                if i == 0:
                    close = row[4]
                if i == 2:
                    open = row[1]
                high = max(high, row[2])
                low = min(low, row[3])
                r.delete(key)
        output_list = [int(time.time() * 1000), open, high, low, close, volume]
        if not r.exists(str(int((time.time() // 60) * 60000)) + symbol + "-15m"):
            r.set(str(int((time.time() // 60) * 60000)) + symbol + "-15m", str(json.dumps(output_list)))
            df = pd.DataFrame([output_list], columns=['t', 'o', 'h', 'l', 'c', 'v'])
            library.write(symbol + '-15m', df)

    return "15m job executed successfully"


@crontab.job(minute="*/30")
@app.route('/get30m')
def process_30m_data():
    time.sleep(40)
    r = redis.Redis(host='localhost', port=6379, db=0)
    library = store['BINANCE_TEST']
    symbols = json.loads(r.get("symbols"))
    for symbol in symbols:
        volume = 0.0
        high = -sys.maxsize
        low = sys.maxsize
        open = 0.0
        close = 0.0
        for i in range(0, 2):
            time_epochs = int((time.time() // 60 - i * 15) * 60000)
            key = str(time_epochs) + symbol + "-15m"
            if r.exists(key):
                li = r.get(key)
                row = json.loads(li)
                volume = volume + row[5]
                if i == 0:
                    close = row[4]
                if i == 1:
                    open = row[1]
                high = max(high, row[2])
                low = min(low, row[3])
                r.delete(key)
        output_list = [(int(time.time() // 60) * 60000), open, high, low, close, volume]
        if not r.exists(str(int((time.time() // 60) * 60000)) + symbol + "-30m"):
            r.set(str(int((time.time() // 60) * 60000)) + symbol + "-30m", str(json.dumps(output_list)))
            pd.DataFrame([output_list], columns=['t', 'o', 'h', 'l', 'c', 'v'])
            library.write(symbol + '-30m', pd)

    return "30m job executed successfully"


@crontab.job(minute="*/60")
@app.route('/get60m')
def process_60m_data():
    time.sleep(40)
    symbols = json.loads(r.get("symbols"))
    r = redis.Redis(host='localhost', port=6379, db=0)
    library = store['BINANCE_TEST']
    for symbol in symbols:
        volume = 0.0
        high = -sys.maxsize
        low = sys.maxsize
        open = 0.0
        close = 0.0
        for i in range(0, 2):
            time_epochs = int((time.time() // 60 - i * 30) * 60000)
            key = str(time_epochs) + symbol + "-30m"
            if r.exists(key):
                li = r.get(key)
                row = json.loads(li)
                volume = volume + row[5]
                if i == 0:
                    close = row[4]
                if i == 1:
                    open = row[1]
                high = max(high, row[2])
                low = min(low, row[3])
                r.delete(key)
        output_list = [(int(time.time() // 60) * 60000), open, high, low, close, volume]
        if not r.exists(str(int((time.time() // 60) * 60000)) + symbol + "-60m"):
            r.set(str(int((time.time() // 60) * 60000)) + symbol + "-60m", str(json.dumps(output_list)))
            pd.DataFrame([output_list], columns=['t', 'o', 'h', 'l', 'c', 'v'])
            library.write(symbol + '-60m', pd)

    return "60m job executed successfully"


if __name__ == "__main__":
    binance = ccxt.binance()
    symbols = []
    for row in binance.fetch_markets():
        if row['active']:
            symbols.append(row['symbol'])
    r = redis.Redis(host='localhost', port=6379, db=0)
    r.set("symbols", str(json.dumps(symbols)))
    print(len(symbols))
    app.run(debug=True)
