# flask app to crawl on binance exchange and fetch kline data so as to store in local db

from arctic.date import DateRange
from flask import Flask
import ccxt
import time
import redis
from flask_crontab import Crontab
import json
import sys
import concurrent.futures
import pandas as pd
import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pymongo import MongoClient

app = Flask(__name__)
crontab = Crontab(app)

client = MongoClient("localhost")
app.store = client['Arctic_data']


# cron job to fetch the 1 minute result for all markets
# from the binance exchange and store them in redis cache for fast processing for higher intervals
@crontab.job()
@app.route('/get1m')
def process_1m_data():
    from flask import current_app as app
    count = 0

    def _fetch_result(symbol, store):
        t1 = time.time()
        ret = []
        data = binance.fetch_ohlcv(symbol, "1m",
                                   int((time.time() // 60 - 1) * 60000))  # fetching ohlcv data from binance
        ret.append(time.time()-t1)
        t1 = time.time()
        ret.append(time.time() - t1)
        t1 = time.time()
        if len(data) > 0:
            if not r.exists(str((int(start)//60)*60) + symbol + "-1m"):  # check so as to prevent duplicate data in same interval
                try:
                    r.set(str((int(start)//60)*60) + symbol + "-1m",
                          str(json.dumps(data[0])), ex=10 * 60)  # updating redis cache with the fetched interval
                    ret.append(time.time() - t1)
                    t1 = time.time()
                    # data[0][0] = pd.to_datetime(data[0][0], unit='ms') # converting epochs to timestamp utc
                    li = data[0]
                    df = {"t": li[0], 'o': li[1], 'h': li[2], 'l': li[3], 'c': li[4], 'v': li[5]}
                    ret.append(time.time() - t1)
                    t1 = time.time()
                    # library.append(symbol + "-1m", df, upsert=True)  # writing dataframe to the arctic db
                    mycol = store[symbol+"-1m"]
                    mycol.insert_one(df)
                    ret.append(time.time() - t1)
                    t1 = time.time()
                except Exception as e:
                    print(e)

        # m.close()
        return ret

    try:
        r = redis.Redis(host='localhost', port=6379, db=0)  # Redis connection setup
        binance = ccxt.binance()
        start = time.time()
        symbols = json.loads(r.get("symbols"))  # Loading active markets form redis
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(symbols)) as executor:  # Executing fetch script in concurrent manner
            market_workers = {executor.submit(_fetch_result, symbol, app.store):
                                  symbol for symbol in symbols}
        end = time.time()
        print(f'{end - start:.2f}')
        print("Success")
    except Exception as e:
        print(e)
    return "Job 1m Triggered successfully"


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
    from flask import current_app as app
    start = time.time()
    r = redis.Redis(host='localhost', port=6379, db=0)
    symbols = json.loads(r.get("symbols"))
    start_5_min =(start // 60) // 5
    start_5_min = int(start_5_min - 1)

    for symbol in symbols:
        volume = 0.0
        high = -sys.maxsize
        low = sys.maxsize
        open = 0.0
        close = 0.0
        for i in range(start_5_min * 5, (start_5_min*5) + 5):
            time_epochs = int(i * 60)  # Fetching data for the last 5 minutes
            key = (str(time_epochs) + symbol + "-1m")

            if r.exists(key):  # Check to avoid duplicate elements
                li = r.get(key)
                row = json.loads(li)
                volume = volume + row[5]
                if i == (start_5_min * 5) + 4:
                    close = row[4]
                if i == start_5_min * 5:
                    open = row[1]
                high = max(high, row[2])
                low = min(low, row[3])
                r.delete(key)  # freeing the cache space after use
        output_list = [int(start_5_min*5*60), open, high, low, close, volume]
        if not r.exists(str(start_5_min*5*60) + symbol + "-5m"):
            r.set(str(start_5_min * 5 * 60) + symbol + "-5m", str(json.dumps(output_list)), ex=20 * 60)
            df = {"t": output_list[0], 'o': output_list[1], 'h': output_list[2], 'l': output_list[3],
                  'c': output_list[4], 'v': output_list[5]}
            mycol = app.store[symbol + "-5m"]
            mycol.insert_one(df)
            # library.append(symbol + '-5m', df, upsert=True)

    end = time.time()
    print(f'{end - start:.2f}')
    return "5m Job executed successfully"


# cron job to fetch the 15 minute result for all markets from the redis cache
# and storing them in db
# Job Triggers in every 15 minute
@crontab.job(minute="*/15")
@app.route('/get15m')
def process_15m_data():
    from flask import current_app as app
    start = time.time()
    r = redis.Redis(host='localhost', port=6379, db=0)
    symbols = json.loads(r.get("symbols"))
    start_15_min = (start // 60) // 15
    start_15_min = int(start_15_min - 1)
    for symbol in symbols:
        volume = 0.0
        high = -sys.maxsize
        low = sys.maxsize
        open = 0.0
        close = 0.0
        for i in range(start_15_min * 3, (start_15_min*3) + 3):  # Fetching data from cache for last 15 minutes from 5 minute interval
            time_epochs = int(i*5*60)
            key = str(time_epochs) + symbol + "-5m"
            if r.exists(key):  # check to avoid duplicate elements
                li = r.get(key)
                row = json.loads(li)
                volume = volume + row[5]
                if i == (start_15_min*3) + 2:
                    close = row[4]
                if i == start_15_min * 3:
                    open = row[1]
                high = max(high, row[2])
                low = min(low, row[3])
                r.delete(key)  # Freeing cache after use
        output_list = [int(start_15_min*15*6), open, high, low, close, volume]
        if not r.exists(str(start_15_min*15*60) + symbol + "-15m"):
            r.set(str(start_15_min*15*60) + symbol + "-15m", str(json.dumps(output_list)), ex=40 * 60)
            df = {"t": output_list[0], 'o': output_list[1], 'h': output_list[2], 'l': output_list[3],
                  'c': output_list[4], 'v': output_list[5]}
            mycol = app.store[symbol + "-15m"]
            mycol.insert_one(df)
            # library.append(symbol + '-15m', df, upsert=True)

    return "15m job executed successfully"


# cron job to fetch the 30 minute result for all markets from the redis cache
# and store them in db
# Job Triggers in every 30 minute
@crontab.job(minute="*/30")
@app.route('/get30m')
def process_30m_data():
    start = time.time()
    r = redis.Redis(host='localhost', port=6379, db=0)
    symbols = json.loads(r.get("symbols"))
    start_30_min = (start // 60) // 30
    start_30_min = int(start_30_min - 1)
    for symbol in symbols:
        volume = 0.0
        high = -sys.maxsize
        low = sys.maxsize
        open = 0.0
        close = 0.0
        for i in range(start_30_min * 2, (start_30_min*2) + 2):  # Fetching Data for last 15 minute interval
            time_epochs = int(i * 15 * 60)
            key = str(time_epochs) + symbol + "-15m"
            if r.exists(key):
                li = r.get(key)
                row = json.loads(li)
                volume = volume + row[5]
                if i == (start_30_min*2) + 1:
                    close = row[4]
                if i == start_30_min * 2:
                    open = row[1]
                high = max(high, row[2])
                low = min(low, row[3])
                r.delete(key)
        output_list = [(int(start_30_min * 30 * 60)), open, high, low, close, volume]
        if not r.exists(str(start_30_min * 30 * 60) + symbol + "-30m"):
            r.set(str(start_30_min * 30 * 60) + symbol + "-30m", str(json.dumps(output_list)), ex=70 * 60)
            df = {"t": output_list[0], 'o': output_list[1], 'h': output_list[2], 'l': output_list[3],
                  'c': output_list[4], 'v': output_list[5]}
            mycol = app.store[symbol + "-30m"]
            mycol.insert_one(df)
            # library.append(symbol + '-30m', df, upsert=True)

    return "30m job executed successfully"


# cron job to fetch the 60 minute result for all markets from the redis cache
# and store them in db
# Job Triggers in every 60 minute
@crontab.job(minute="*/60")
@app.route('/get60m')
def process_60m_data():
    start = time.time()
    r = redis.Redis(host='localhost', port=6379, db=0)
    symbols = json.loads(r.get("symbols"))
    start_60_min = (start // 60) //60
    start_60_min = int(start_60_min - 1)
    for symbol in symbols:
        volume = 0.0
        high = -sys.maxsize
        low = sys.maxsize
        open = 0.0
        close = 0.0
        for i in range(start_60_min*2, (start_60_min*2) + 2):
            time_epochs = int(i * 30 * 60)
            key = str(time_epochs) + symbol + "-30m"
            if r.exists(key):
                li = r.get(key)
                row = json.loads(li)
                volume = volume + row[5]
                if i == (start_60_min*2) + 1:
                    close = row[4]
                if i == start_60_min*2:
                    open = row[1]
                high = max(high, row[2])
                low = min(low, row[3])
                r.delete(key)
        output_list = [(int(start_60_min * 60 * 60)), open, high, low, close, volume]
        if not r.exists(str(start_60_min * 60 * 60) + symbol + "-60m"):
            r.set(str(start_60_min * 60 * 60) + symbol + "-60m", str(json.dumps(output_list)), ex=100 * 60)
            df = {"t": output_list[0], 'o': output_list[1], 'h': output_list[2], 'l': output_list[3],
                  'c': output_list[4], 'v': output_list[5]}
            mycol = app.store[symbol + "-60m"]
            mycol.insert_one(df)

    return "60m job executed successfully"


# cron job to check the quality of data fetched
# mails daily report
# Job Triggers in every 24 minute starting at 00:00
@crontab.job(minute="0", hour="0")
@app.route('/check_data_quality')
def check_data_quality():
    from flask import current_app as app
    r = redis.Redis(host='localhost', port=6379, db=0)
    symbols = json.loads(r.get("symbols"))
    query_for_1m = {
        "t": {
            "$gte": 1609785000000,
            "$lte": 1609821000000
        }
    }
    query = {
        "t": {
            "$gte": 1609785000,
            "$lte": 1609821000
        }
    }
    query_for_15m = {
        "t": {
            "$gte": 160978500,
            "$lte": 160982100
        }
    }

    min = 480
    count = 0
    with open("report.txt", 'w') as rep:  # File that is attached in the mail
        for symbol in symbols:
            try:
                # calculating missing data due to 1m job
                rep.write("\n\nFor symbol " + symbol + "\n")
                df = app.store[symbol+"-1m"].count_documents(query_for_1m)
                rep.write(symbol + " percentage of 1m data fetched: " + str(df * 100 / min) + "\n")

                # calculating missing data due to 5m job
                df = app.store[symbol + "-5m"].count_documents(query)
                rep.write(symbol + " percentage of 5m data fetched: " + str(df * 100 / (min//5)) + "\n")

                # calculating missing data due to 15m job
                df = app.store[symbol + "-15m"].count_documents(query_for_15m)
                rep.write(symbol + " percentage of 15m data fetched: " + str(df * 100 / (min // 15)) + "\n")

                # calculating missing data due to 30m job
                df = app.store[symbol + "-30m"].count_documents(query)
                rep.write(symbol + " percentage of 30m data fetched: " + str(df * 100 / (min // 30)) + "\n")

                # calculating missing data due to 60m job
                df = app.store[symbol + "-60m"].count_documents(query)
                rep.write(symbol + " percentage of 60m data fetched: " + str(df * 100 / (min // 60)) + "\n")

            except Exception as e:
                print(e)
                count += 1
    try:
        print(count)

        sender_address = 'saksham.jain2109@gmail.com'
        sender_pass = '****'
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
