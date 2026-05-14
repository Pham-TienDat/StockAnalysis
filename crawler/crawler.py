from confluent_kafka import Producer
from vnstock import *
from datetime import datetime, timedelta
from time import sleep
import threading

# Hàm gửi dữ liệu JSON vào Kafka với mã chứng khoán
def produce_kafka_json(bootstrap_servers, topic_name, symbol, json_message):
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    producer.produce(topic_name, value=json_message.encode('utf-8'), key=symbol.encode('utf-8'), callback=delivery_report)
    producer.flush()

# Hàm callback cho việc gửi tin nhắn
def delivery_report(err, msg):
    """Callback được gọi khi tin nhắn được gửi thành công hoặc gặp lỗi."""
    if err is not None:
        print('Gửi tin nhắn thất bại: {}'.format(err))
    else:
        print('Tin nhắn được gửi thành công: {}'.format(msg.key().decode('utf-8')))

# Hàm lấy dữ liệu chứng khoán cho một mã cụ thể
def get_stock_data(symbol):
    today = datetime.now()
    start_date_this_week = today - timedelta(days=today.weekday())
    start_date_last_week = start_date_this_week - timedelta(days=7)
    end_date_last_week = start_date_last_week + timedelta(days=6)
    start_date_last_week_str = start_date_last_week.strftime('%Y-%m-%d')
    end_date_last_week_str = end_date_last_week.strftime('%Y-%m-%d')
    
    df = stock_historical_data(symbol=symbol,
                               start_date=start_date_last_week_str,
                               end_date=end_date_last_week_str,
                               resolution='1H',
                               type='stock',
                               beautify=True)
    # Chuyển dữ liệu thành JSON với thêm thông tin về mã chứng khoán
    df['time'] = pd.to_datetime(df['time'])
    # df['time'] = df['time'].dt.strftime('%Y-%m-%d')
    json_data = df.to_json(date_format='iso', orient='records')
    return json_data

def get_stock_data_intraday(symbol): 
    df = stock_intraday_data(
            symbol=symbol, page_size=1, investor_segment=True
        )
    df['time'] = pd.to_datetime(df['time'])
    json_data = df.to_json(date_format='iso', orient='records')
    return json_data

def jobCrawlVn30Data(kafka_topic, bootstrap_servers):
    stock_array = ["ACB","BCM","BID","BVH","CTG","FPT","GAS","GVR","DHB","HPG","MBB","MSN",
               "MWG","PLX","POW","SAB","SHB","SSB","TCB","TPB","VCB","VHM","VIB","VIC","VJC","VNM","VPB","VRE", "SSI", "HDB"]
    while True:
        for symbol in stock_array:
            stock_data = get_stock_data(symbol)  # Lấy dữ liệu cho mã chứng khoán hiện tại
            print(stock_data)
            produce_kafka_json(bootstrap_servers, kafka_topic, symbol, stock_data)  # Gửi dữ liệu vào Kafka với mã chứng khoán
            time.sleep(2)  # Chờ 2 giây trước khi lấy dữ liệu cho mã chứng khoán tiếp theo
        break

def jobCrawlStockDataRealtime(symbol, kafka_topic, bootstrap_servers):
    index = 1
    while True: 
        stock_data = get_stock_data_intraday('ACB')
        print(str(index) +  ": \n")
        print(stock_data)
        produce_kafka_json(bootstrap_servers, kafka_topic, symbol, stock_data)
        sleep(30)
        index = index+1

if __name__ == "__main__":
    bootstrap_servers = 'kafka:9092'  # Thay thế bằng địa chỉ Kafka broker của bạn
    kafka_topic_vn30 = 'vn30'  # Thay thế bằng tên Kafka topic của bạn
    kafka_topic_realtime = 'stock_realtime'

    t1 = threading.Thread(target=jobCrawlVn30Data, args=(kafka_topic_vn30, bootstrap_servers))
    t2 = threading.Thread(target=jobCrawlStockDataRealtime, args=('ACB', kafka_topic_realtime, bootstrap_servers))

    t1.start()
    t2.start()

    t1.join()
    t2.join()



