from flask import Flask
import ccxt
import psycopg2
import datetime
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
store.initialize_library("BINANCE5")
symbols = []


# mongo = PyMongo(app)


@crontab.job()
@app.route('/get1m')
def index():
    count = 0
    def _fetch_result(symbol):
        data = binance.fetch_ohlcv(symbol, "1m", int((time.time() // 60 - 1) * 60000))
        r = redis.Redis(host='localhost', port=6379, db=0)
        library = store['BINANCE5']
        # print(data)
        if len(data) > 0:
            if not r.exists(str(int((time.time() // 60) * 60000)) + symbol + "-1m"):
                df = pd.DataFrame([data[0]], columns=['t', 'o', 'h', 'l', 'c', 'v'])
                r.set(str(int((time.time() // 60) * 60000)) + symbol + "-1m", str(json.dumps(data[0])))
                library.append(symbol + "-1m", df)
                # print(df)
            return data[0]

        return []
    try:
        exchange_id = 'binance'
        r = redis.Redis(host='localhost', port=6379, db=0)
        library = store['BINANCE2']
        binance = ccxt.binance()
        start = time.time()
        symbols=json.loads(r.get("symbols"))
        print(symbols)
        # print(int((time.time()//60-1)*60000))
        dd = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(symbols)) as executor:
            market_workers = {executor.submit(_fetch_result, symbol):
                                  symbol for symbol in symbols}
        end = time.time()
        print(f'{end - start:.2f}')
        # for symbol in symbols:
        #     count+=1
        #     print(count)
        #     dd[symbol]=binance.fetch_ohlcv(symbol, "1m", int((time.time() // 60 - 1) * 60000))
        # print(dd)
        # dict = binance.fetch_ohlcv("BTC/USDT", "1m", int((time.time() // 60 - 1) * 60000))
        #
        # for row in dict:
        #     try:
        #         print(row)
        #         count = count + 1
        #         data = json.dumps(row)
        #         # print(data)
        #         # print(r.exists(str(int((time.time()//60-1)*60000))))
        #         if not r.exists(str(int((time.time() // 60) * 60000)) + "-1m"):
        #             r.set(str(int((time.time() // 60) * 60000)) + "-1m", str(data))
        #             library.write('BTC/USDT-1m', row)
        #         # mongo.db.test.insert_one(data)
        #
        #         break
        #     except Exception as e:
        #         print(e)
        #
        # item = library.read('BTC/USDT-1m')
        # print(item.data)
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
def test():
    # time.sleep(30)
    start = time.time()
    r = redis.Redis(host='localhost', port=6379, db=0)
    library = store['BINANCE2']
    for symbol in symbols:
        volume = 0.0
        high = -sys.maxsize
        low = sys.maxsize
        op = 0.0
        close = 0.0
        count = 0
        for i in range(0, 5):
            tt = int((time.time() // 60 - i) * 60000)
            # print(symbol)
            key = (str(tt) + symbol + "-1m")

            if r.exists(key):
                li = r.get(key)
                temp = json.loads(li)
                count += 1
                # print(temp, temp[0])
                volume = volume + temp[5]
                if i == 0:
                    close = temp[4]
                if i == 4:
                    op = temp[1]
                high = max(high, temp[2])
                low = min(low, temp[3])
                r.delete(key)
        print(str(symbol) + " " + str(count))
        output_list = [int((time.time() // 60) * 60000), op, high, low, close, volume]
        if not r.exists(str(int((time.time() // 60) * 60000)) + symbol + "-5m"):
            r.set(str(int((time.time() // 60) * 60000)) + symbol + "-5m", str(json.dumps(output_list)))
            df = pd.DataFrame(output_list, columns=['t', 'o', 'h', 'l', 'c', 'v'])
            library.write(symbol + '-5m', df)

    end = time.time()
    print(f'{end - start:.2f}')
    return str(json.dumps(output_list))


@crontab.job(minute="*/15")
@app.route('/get15m')
def process_15m_data():
    time.sleep(20)
    r = redis.Redis(host='localhost', port=6379, db=0)
    library = store['BINANCE']
    for symbol in symbols:
        volume = 0.0
        high = -sys.maxsize
        low = sys.maxsize
        op = 0.0
        close = 0.0
        for i in range(0, 3):
            tt = int((time.time() // 60 - i * 5) * 60000)
            li = r.get(str(tt) + symbol + "-5m")
            print(tt)
            if li:
                temp = json.loads(li)
                print(temp, temp[0])
                volume = volume + temp[5]
                if i == 0:
                    close = temp[4]
                if i == 2:
                    op = temp[1]
                high = max(high, temp[2])
                low = min(low, temp[3])
        output_list = [int(time.time() * 1000), op, high, low, close, volume]
        if not r.exists(str(int((time.time() // 60) * 60000)) + symbol + "-15m"):
            r.set(str(int((time.time() // 60) * 60000)) + symbol + "-15m", str(json.dumps(output_list)))
            df = pd.DataFrame(output_list, columns=['t', 'o', 'h', 'l', 'c', 'v'])
            library.write(symbol + '-15m', df)

    return str(json.dumps(output_list))


@crontab.job(minute="*/30")
@app.route('/get30m')
def process_30m_data():
    time.sleep(40)
    r = redis.Redis(host='localhost', port=6379, db=0)
    library = store['BINANCE2']
    for symbol in symbols:
        volume = 0.0
        high = -sys.maxsize
        low = sys.maxsize
        op = 0.0
        close = 0.0
        for i in range(0, 2):
            tt = int((time.time() // 60 - i * 15) * 60000)
            li = r.get(str(tt) + symbol + "-15m")
            print(tt)
            if li:
                temp = json.loads(li)
                print(temp, temp[0])
                volume = volume + temp[5]
                if i == 0:
                    close = temp[4]
                if i == 1:
                    op = temp[1]
                high = max(high, temp[2])
                low = min(low, temp[3])
        output_list = [(int(time.time() // 60) * 60000), op, high, low, close, volume]
        if not r.exists(str(int((time.time() // 60) * 60000)) + symbol + "-30m"):
            r.set(str(int((time.time() // 60) * 60000)) + symbol + "-30m", str(json.dumps(output_list)))
            pd.DataFrame(output_list, columns=['t', 'o', 'h', 'l', 'c', 'v'])
            library.write(symbol + '-30m', pd)

    return str(json.dumps(output_list))


@crontab.job(minute="*/60")
@app.route('/get60m')
def process_60m_data():
    time.sleep(40)
    r = redis.Redis(host='localhost', port=6379, db=0)
    library = store['BINANCE2']
    for symbol in symbols:
        volume = 0.0
        high = -sys.maxsize
        low = sys.maxsize
        op = 0.0
        close = 0.0
        for i in range(0, 2):
            tt = int((time.time() // 60 - i * 30) * 60000)
            li = r.get(str(tt) + symbol + "-15m")
            print(tt)
            if li:
                temp = json.loads(li)
                print(temp, temp[0])
                volume = volume + temp[5]
                if i == 0:
                    close = temp[4]
                if i == 1:
                    op = temp[1]
                high = max(high, temp[2])
                low = min(low, temp[3])
        output_list = [(int(time.time() // 60) * 60000), op, high, low, close, volume]
        if not r.exists(str(int((time.time() // 60) * 60000)) + symbol + "-60m"):
            r.set(str(int((time.time() // 60) * 60000)) + symbol + "-60m", str(json.dumps(output_list)))
            pd.DataFrame(output_list, columns=['t', 'o', 'h', 'l', 'c', 'v'])
            library.write(symbol + '-60m', pd)

    return str(json.dumps(output_list))


@app.route('/test')
def tt():
    li = []
    library = store['BINANCE5']
    for symbol in symbols:
        try:
            print(symbol)
            df = library.read(symbol + "-1m").data
            print(len(df.index))

        except:
            li.append(symbol)

    return "li"


if __name__ == "__main__":
    binance = ccxt.binance()
    for row in binance.fetch_markets():
        if row['active']:
            symbols.append(row['symbol'])
    r = redis.Redis(host='localhost', port=6379, db=0)
    r.set("symbols",str(json.dumps(symbols)))
    print(len(symbols))
    app.run(debug=True)
    
