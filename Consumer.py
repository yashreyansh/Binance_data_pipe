from confluent_kafka import Consumer, KafkaException
import requests, json, datetime, time
import signal
import sys

from azure.storage.filedatalake import DataLakeServiceClient


with open("config.json","r") as f:
    config = json.load(f)
    

running = True
storage_account = config["StorageAccount"]
storage_Account_key = config["StorageAccountKey"]
service_client = DataLakeServiceClient(
    account_url = f"https://{storage_account}.dfs.core.windows.net",
    credential = storage_Account_key
)
container = config["Container"]

def save_to_adls(data,file_name):
    file_client = service_client.get_file_client(container, file_name)
    data = "\n".join([json.dumps(record) for record in data])
    #data = json.dumps(data, indent=2)
    file_client.upload_data(data, overwrite=True)
    #print(data[:100])
    print(f"File saved : {file_name}")
    
def shutdown_handler(sig, frame):
    global running
    print("Shutdown signal received")
    running = False


# ------------------------------------------------------------------#
def trades_send(trade):
    if trade ==[]:
        print("No trade to send...")
        return
    timestamp = datetime.datetime.now().strftime("%Y%m%d_H%M%S")
    file_name = f'trade/{timestamp}.json'
    save_to_adls(trade, file_name)
    #print(trade)


def price_data_send(price):
    if price == [] :
        print("No prices to send...")
        return
    timestamp = datetime.datetime.now().strftime("%Y%m%d_H%M%S")
    file_name = f'BTC_prices/{timestamp}.json'
    save_to_adls(price, file_name)

# ------------------------------------------------------------------#    
if __name__== "__main__":
    
    signal.signal(signal.SIGINT, shutdown_handler)   # KeyboardInterrupt (Ctrl+C)
    signal.signal(signal.SIGTERM, shutdown_handler)  # `kill` or container stop
    
        
    kafka_user = config["kafka_user"]
    kafka_api_pass = config["kafka_api_pass"]
    bootstrap_server = config["bootstrap_server"]
    kafka_topic = "binance_BTC_prices"
    conf = {
        'bootstrap.servers': bootstrap_server,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': kafka_user,
        'sasl.password': kafka_api_pass,
        'group.id': 'Binance_kafka_grp',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([config["BTC_Trade_topic"], config["BTC_price_topic"]])

    try:
        trade_count = 0
        price_data_count = 0
        trade_buffer = []
        price_buffer = []
        while running:
            msg = consumer.poll(3.0)   # polling every 3 seconds
            print("Polling")
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
######################################################            
            if msg.topic() == config["BTC_Trade_topic"]:
                print(f"Data received: {config['BTC_Trade_topic']} ")
                #print(f"{msg.value().decode('utf-8')}")
                trades = json.loads(msg.value().decode('utf-8'))  # load the value as json
                for event in trades["events"]:
                    event["ingest_time"] = datetime.datetime.utcnow().isoformat()
                    trade_buffer.append(json.dumps(event))
                    #print(trade_count)   
                    trade_count +=1

                    if trade_count==config["consumer_trade_batch_size"]:
                        trades_send(trade_buffer)    # send out data if we have 5 trades
                        trade_count=0
                        trade_buffer=[]
######################################################                    
            if msg.topic() == config["BTC_price_topic"]:
                print(f"Data received: {config['BTC_price_topic']} ")
                #print(f"{msg.value().decode('utf-8')}")
                price = json.loads(msg.value().decode('utf-8'))  # load the value as json
                price["ingest_time"] = datetime.datetime.utcnow().isoformat()
                #price_buffer.append(json.dumps(price))
                price_buffer.append(price)
                #print(price)
                price_data_count +=1

                if price_data_count==config["consumer_price_batch_size"]:
                    price_data_send(price_buffer)
                    print(price_buffer)
                    price_data_count=0
                    price_buffer=[]     
                    
#############                     
                
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
        pass
    finally:
        print("Sending data before closing...")
        price_data_send(price_buffer)
        trades_send(trade_buffer)
        print("Closing consumer!!")
        consumer.close()
















        