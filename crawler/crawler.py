from confluent_kafka import Producer
from vnstock.api.quote import Quote
import pandas as pd
from datetime import datetime, timedelta
from time import sleep
import threading
import os

# Đọc API key từ biến môi trường (được inject qua docker-compose env_file)
_API_KEY = os.environ.get('VNSTOCK_API_KEY', '')
if _API_KEY:
    os.environ['VNSTOCK_API_KEY'] = _API_KEY
    print(f'[config] Dùng API key: {_API_KEY[:8]}...  (60 req/phút)')
    CRAWL_DELAY = 1   # 60 req/phút với Community key
else:
    print('[config] Không có API key, dùng Guest tier (20 req/phút)')
    CRAWL_DELAY = 4   # 15 req/phút, an toàn dưới giới hạn 20

def produce_kafka_json(bootstrap_servers, topic_name, symbol, json_message):
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    producer.produce(topic_name, value=json_message.encode('utf-8'), key=symbol.encode('utf-8'), callback=delivery_report)
    producer.flush()

def delivery_report(err, msg):
    if err is not None:
        print('Gửi tin nhắn thất bại: {}'.format(err))
    else:
        print('Tin nhắn được gửi thành công: {}'.format(msg.key().decode('utf-8')))

def _resolve_date_range():
    """Trả về (start_str, end_str) từ env vars hoặc mặc định là tuần hiện tại."""
    start = os.environ.get('CRAWL_START_DATE', '').strip()
    end   = os.environ.get('CRAWL_END_DATE', '').strip()
    if start and end:
        return start, end
    today = datetime.now()
    start = (today - timedelta(days=today.weekday())).strftime('%Y-%m-%d')  # thứ 2 tuần này
    end   = today.strftime('%Y-%m-%d')
    return start, end

def get_stock_data(symbol):
    start_str, end_str = _resolve_date_range()
    interval = os.environ.get('CRAWL_INTERVAL', '1H').strip()

    print(f'[vn30] {symbol}: {start_str} → {end_str} ({interval})')
    q = Quote(symbol=symbol, source='VCI')
    df = q.history(start=start_str, end=end_str, interval=interval)
    df['ticker'] = symbol
    df['time'] = pd.to_datetime(df['time'])
    return df.to_json(date_format='iso', orient='records')

def get_stock_data_intraday(symbol):
    q = Quote(symbol=symbol, source='kbs')
    df = q.intraday(page_size=100)
    df['ticker'] = symbol
    df['time'] = pd.to_datetime(df['time'])
    return df.to_json(date_format='iso', orient='records')

def jobCrawlVn30Data(kafka_topic, bootstrap_servers):
    stock_array = ["ACB","BCM","BID","BVH","CTG","FPT","GAS","GVR","DGC","HPG","MBB","MSN",
               "MWG","PLX","POW","SAB","SHB","SSB","TCB","TPB","VCB","VHM","VIB","VIC","VJC","VNM","VPB","VRE","SSI","HDB"]
    while True:
        for symbol in stock_array:
            try:
                stock_data = get_stock_data(symbol)
                print(f'[vn30] {symbol}: {len(stock_data)} bytes → Kafka')
                produce_kafka_json(bootstrap_servers, kafka_topic, symbol, stock_data)
            except Exception as e:
                print(f'[vn30] Lỗi khi crawl {symbol}: {e}')
            sleep(CRAWL_DELAY)
        print('[vn30] Xong batch, chờ 1 tiếng...')
        sleep(3600)

def jobCrawlStockDataRealtime(symbol, kafka_topic, bootstrap_servers):
    index = 1
    while True:
        try:
            stock_data = get_stock_data_intraday(symbol)
            print(f'[realtime] #{index} {symbol}: {len(stock_data)} bytes → Kafka')
            produce_kafka_json(bootstrap_servers, kafka_topic, symbol, stock_data)
        except Exception as e:
            print(f'[realtime] Lỗi khi crawl {symbol}: {e}')
        sleep(30)
        index += 1

if __name__ == "__main__":
    bootstrap_servers = 'kafka:9092'
    kafka_topic_vn30 = 'vn30'
    kafka_topic_realtime = 'stock_realtime'

    t1 = threading.Thread(target=jobCrawlVn30Data, args=(kafka_topic_vn30, bootstrap_servers), name='jobCrawlVn30Data')
    t2 = threading.Thread(target=jobCrawlStockDataRealtime, args=('ACB', kafka_topic_realtime, bootstrap_servers), name='jobCrawlStockDataRealtime')

    t1.start()
    t2.start()

    t1.join()
    t2.join()
