from confluent_kafka import Producer
import requests, json, datetime, time
import signal
import sys

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to Topic: {} , partition:{}'.format(msg.topic(), msg.partition()))

def shutdown_handler(sig, frame):
    global running
    print("Shutdown signal received")
    running = False


running = True
if __name__== "__main__":

    signal.signal(signal.SIGINT, shutdown_handler)   # KeyboardInterrupt (Ctrl+C)
    signal.signal(signal.SIGTERM, shutdown_handler)  # `kill` or container stop
    
    
    with open("config.json","r") as f:
        config = json.load(f)
    kafka_user = config["kafka_user"]
    kafka_api_pass = config["kafka_api_pass"]
    bootstrap_server = config["bootstrap_server"]
    
    kafka_topic = "binance_BTC_prices"
    conf = {
        'bootstrap.servers': bootstrap_server,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': kafka_user,
        'sasl.password': kafka_api_pass
    }
    i = 0
    
    producer = Producer(conf)
    try:
        while running:
            response = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT")
            data_dict  = response.json()
            data_dict["time"] = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
            data = json.dumps(data_dict)
            
            producer.produce(kafka_topic, value=data, callback = delivery_report)
            producer.flush()
            time.sleep(2)
    
            i+=1
            if i==config["temp_price_runs"]:
                break
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
        pass
    finally:
        print("Sending pending messages before closing.....")
        producer.flush()
        print("Closing price producer....")

###############################################################