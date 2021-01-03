# flask app to crawl on binance exchange and fetch kline data so as to store in local db

from arctic.date import DateRange
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
import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

app = Flask(__name__)
crontab = Crontab(app)
store = Arctic("localhost")  # connecting to local mongo server
store.initialize_library("BINANCE")  # initializing library for the arctic


# cron job to fetch the 1 minute result for all markets
# from the binance exchange and store them in redis cache for fast processing for higher intervals
@crontab.job()
@app.route('/get1m')
def process_1m_data():
    count = 0

    def _fetch_result(symbol):
        data = binance.fetch_ohlcv(symbol, "1m",
                                   int((time.time() // 60 - 1) * 60000))  # fetching ohlcv data from binance
        store = Arctic("localhost")  # connecting to local mongo server
        library = store['BINANCE']
        if len(data) > 0:
            if not r.exists(str(int((start // 60) * 60000)) + symbol + "-1m"):  # check so as to prevent duplicate data in same interval
                try:

                    r.set(str(int((start // 60) * 60000)) + symbol + "-1m",
                          str(json.dumps(data[0])), ex=10*60)  # updating redis cache with the fetched interval
                    data[0][0] = pd.to_datetime(data[0][0] / 1000, unit='s').tz_localize(
                        "UTC")  # converting epochs to timestamp utc
                    df = pd.DataFrame([data[0]], columns=['t', 'o', 'h', 'l', 'c', 'v'])
                    df.set_index('t')
                    library.write(symbol + "-1m", df)  # writing dataframe to the arctic db
                except Exception as e:
                    print(e)

        return

    try:
        r = redis.Redis(host='localhost', port=6379, db=0)  # Redis connection setup
        binance = ccxt.binance()
        start = time.time()
        symbols = json.loads(r.get("symbols"))  # Loading active markets form redis
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(symbols)) as executor:  # Executing fetch script in concurrent manner
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

# cron job to fetch the 5 minute result for all markets from the redis cache
# and storing them in db
# Job Triggers in every 5 minute
@crontab.job(minute="*/5")
@app.route('/get5m')
def process_5m_data():
    time.sleep(10)  # Delay to provide mutual exclusiveness for 1m job
    start = time.time()
    r = redis.Redis(host='localhost', port=6379, db=0)
    store = Arctic("localhost")  # connecting to local mongo server
    library = store['BINANCE']
    symbols = json.loads(r.get("symbols"))
    for symbol in symbols:
        volume = 0.0
        high = -sys.maxsize
        low = sys.maxsize
        open = 0.0
        close = 0.0
        for i in reversed(range(1, 6)):
            time_epochs = int((time.time() // 60 - i) * 60000)  # Fetching data for the last 5 minutes
            key = (str(time_epochs) + symbol + "-1m")

            if r.exists(key):  # Check to avoid duplicate elements
                li = r.get(key)
                row = json.loads(li)
                volume = volume + row[5]
                if i == 0:
                    close = row[4]
                if i == 4:
                    open = row[1]
                high = max(high, row[2])
                low = min(low, row[3])
                r.delete(key)  # freeing the cache space after use
        output_list = [int((time.time() // 60) * 60000), open, high, low, close, volume]
        if not r.exists(str(int((start // 60) * 60000)) + symbol + "-5m"):
            r.set(str(int((start // 60) * 60000)) + symbol + "-5m", str(json.dumps(output_list)), ex=20*60)
            output_list[0] = pd.to_datetime(output_list[0] / 1000, unit='s').tz_localize("UTC")
            df = pd.DataFrame([output_list], columns=['t', 'o', 'h', 'l', 'c', 'v'])
            df.set_index('t')
            library.write(symbol + '-5m', df)

    end = time.time()
    print(f'{end - start:.2f}')
    return "5m Job executed successfully"


# cron job to fetch the 15 minute result for all markets from the redis cache
# and storing them in db
# Job Triggers in every 15 minute
@crontab.job(minute="*/15")
@app.route('/get15m')
def process_15m_data():
    time.sleep(25)  # Delay to provide mutual exclusiveness for 5m job
    start = time.time()
    r = redis.Redis(host='localhost', port=6379, db=0)
    store = Arctic("localhost")  # connecting to local mongo server
    library = store['BINANCE']
    symbols = json.loads(r.get("symbols"))
    for symbol in symbols:
        volume = 0.0
        high = -sys.maxsize
        low = sys.maxsize
        open = 0.0
        close = 0.0
        for i in reversed(range(0, 3)):  # Fetching data from cache for last 15 minutes from 5 minute interval
            time_epochs = int((time.time() // 60 - i * 5) * 60000)
            key = str(time_epochs) + symbol + "-5m"
            if r.exists(key):  # check to avoid duplicate elements
                li = r.get(key)
                row = json.loads(li)
                volume = volume + row[5]
                if i == 0:
                    close = row[4]
                if i == 2:
                    open = row[1]
                high = max(high, row[2])
                low = min(low, row[3])
                r.delete(key)  # Freeing cache after use
        output_list = [int(time.time()), open, high, low, close, volume]
        if not r.exists(str(int((start // 60) * 60000)) + symbol + "-15m"):
            r.set(str(int((start // 60) * 60000)) + symbol + "-15m", str(json.dumps(output_list)), ex=40*60)
            output_list[0] = pd.to_datetime(output_list[0], unit='s').tz_localize("UTC")
            df = pd.DataFrame([output_list], columns=['t', 'o', 'h', 'l', 'c', 'v'])  # Dataframe to feed in the db
            df.set_index('t')
            library.write(symbol + '-15m', df)

    return "15m job executed successfully"


# cron job to fetch the 30 minute result for all markets from the redis cache
# and store them in db
# Job Triggers in every 30 minute
@crontab.job(minute="*/30")
@app.route('/get30m')
def process_30m_data():
    time.sleep(35)  # delay to provide space to 15m job
    start = time.time()
    r = redis.Redis(host='localhost', port=6379, db=0)
    store = Arctic("localhost")  # connecting to local mongo server
    library = store['BINANCE']
    symbols = json.loads(r.get("symbols"))
    for symbol in symbols:
        volume = 0.0
        high = -sys.maxsize
        low = sys.maxsize
        open = 0.0
        close = 0.0
        for i in reversed(range(0, 2)):  # Fetching Data for last 15 minute interval
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
        if not r.exists(str(int((start // 60) * 60000)) + symbol + "-30m"):
            r.set(str(int((start // 60) * 60000)) + symbol + "-30m", str(json.dumps(output_list)), ex=70*60)
            output_list[0] = pd.to_datetime(output_list[0] / 1000, unit='s').tz_localize("UTC")
            df = pd.DataFrame([output_list], columns=['t', 'o', 'h', 'l', 'c', 'v'])
            df.set_index('t')
            library.write(symbol + '-30m', df)

    return "30m job executed successfully"


# cron job to fetch the 60 minute result for all markets from the redis cache
# and store them in db
# Job Triggers in every 60 minute
@crontab.job(minute="*/60")
@app.route('/get60m')
def process_60m_data():
    time.sleep(45)  # Delay to provide buffer space to 30 m job
    start = time.time()
    r = redis.Redis(host='localhost', port=6379, db=0)
    symbols = json.loads(r.get("symbols"))
    store = Arctic("localhost")  # connecting to local mongo server
    library = store['BINANCE']
    for symbol in symbols:
        volume = 0.0
        high = -sys.maxsize
        low = sys.maxsize
        open = 0.0
        close = 0.0
        for i in reversed(range(0, 2)):
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
        if not r.exists(str(int((start // 60) * 60000)) + symbol + "-60m"):
            r.set(str(int((start // 60) * 60000)) + symbol + "-60m", str(json.dumps(output_list)), ex=100*60)
            output_list[0] = pd.to_datetime(output_list[0] / 1000, unit='s').tz_localize("UTC")
            df = pd.DataFrame([output_list], columns=['t', 'o', 'h', 'l', 'c', 'v'])
            df.set_index('t')
            library.write(symbol + '-60m', df)

    return "60m job executed successfully"


# cron job to check the quality of data fetched
# mails daily report
# Job Triggers in every 24 minute starting at 00:00
@crontab.job(minute="0", hour="0")
@app.route('/check_data_quality')
def check_data_quality():
    store = Arctic("localhost")  # connecting to local mongo server
    library = store['BINANCE']
    r = redis.Redis(host='localhost', port=6379, db=0)
    symbols = json.loads(r.get("symbols"))
    dr = DateRange(datetime.datetime.utcfromtimestamp(time.time()) - datetime.timedelta(hours=24),
                   datetime.datetime.utcfromtimestamp(time.time()))  # DateRange for last 24 hours in utc timestamp
    count = 0
    with open("report.txt", 'w') as rep:  # File that is attached in the mail
        for symbol in symbols:
            try:
                # calculating missing data due to 1m job
                rep.write("\n\nFor symbol " + symbol + "\n")
                df = library.read(symbol + "-1m", date_range=dr).data
                count = len(df.index)
                if count < 1440:
                    rep.write(symbol + "missed 1m data percentage: " + str((1440 - count) * 100 / 1440) + "\n")

                # calculating missing data due to 5m job
                df = library.read(symbol + "-5m", date_range=dr).data
                count = len(df.index)
                if count < 288:
                    rep.write(symbol + "missed 5m data percentage: " + str((288 - count) * 100 / 288) + "\n")

                # calculating missing data due to 15m job
                df = library.read(symbol + "-15m", date_range=dr).data
                count = len(df.index)
                if count < 96:
                    rep.write(symbol + "missed 15m data percentage: " + str((96 - count) * 100 / 96) + "\n")

                # calculating missing data due to 30m job
                df = library.read(symbol + "-30m", date_range=dr).data
                count = len(df.index)
                if count < 48:
                    rep.write(symbol + "missed 30m data percentage: " + str((48 - count) * 100 / 48) + "\n")

                # calculating missing data due to 60m job
                df = library.read(symbol + "-60m", date_range=dr).data
                count = len(df.index)
                if count < 24:
                    rep.write(symbol + "missed 60m data percentage: " + str((24 - count) * 100 / 24) + "\n")

            except Exception as e:
                print(e)
                count += 1
    try:
        print(count)

        sender_address = 'saksham.jain2109@gmail.com'
        sender_pass = 'Vmc1234$'
        receiver_address = 'jainsaksham36b@gmail.com'
        # Setup the MIME
        message = MIMEMultipart()
        message['From'] = sender_address
        message['To'] = receiver_address
        message['Subject'] = 'Daily report for Fetch data'
        message.attach(MIMEText("Hello\n please find the daily report", 'plain'))
        attach_file_name = 'report.txt'
        attach_file = open(attach_file_name, 'rb')  # Open the file as binary mode
        payload = MIMEBase('application', 'octate-stream')
        payload.set_payload((attach_file).read())
        encoders.encode_base64(payload)  # encode the attachment
        # add payload header with filename
        payload.add_header('Content-Decomposition', 'attachment', filename=attach_file_name)
        message.attach(payload)
        # Create SMTP session for sending the mail
        session = smtplib.SMTP('smtp.gmail.com', 587)  # use gmail with port
        session.starttls()  # enable security
        session.login(sender_address, sender_pass)  # login with mail_id and password
        text = message.as_string()
        session.sendmail(sender_address, receiver_address, text)
        session.quit()
    except Exception as e:
        print(e)
    return "Job ended"


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
