from confluent_kafka import Producer
import requests, json, datetime, time

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to Topic: {} , partition:{}'.format(msg.topic(), msg.partition()))

if __name__== "__main__":

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
    while True:
        response = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT")
        data_dict  = response.json()
        data_dict["time"] = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        data = json.dumps(data_dict)
        
        producer = Producer(conf)
        producer.produce(kafka_topic, value=data, callback = delivery_report)
        producer.flush()
        time.sleep(4)

        i+=1
        if i==5:
            break
    