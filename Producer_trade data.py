import websocket
import json, time, threading
from confluent_kafka import Producer



def run_ws(duration):
    ws = websocket.WebSocketApp(socket,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    
    def stop_ws():
        ws.close()
    timer = threading.Timer(duration, stop_ws)
    timer.start()
    
    ws.run_forever()


def on_message(ws, message):
    #global trades_count
    trade = json.loads(message)
    print(f"TradeID: {trade['t']} | Symbol: {trade['s']} | Price: {trade['p']} | Quantity: {trade['q']}")
    batch.append(trade)
    if len(batch)>=10:
        send_batch_to_kafka(batch)
    #return trade
        
def on_error(ws, error):
    print("Error:", error)

def on_close(ws, close_status_code, close_msg):
    if batch:
        send_batch_to_kafka(batch)
        print("sending rest of the data")
    print("Closed connection")

def on_open(ws):
    print("Connection opened")
    
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def send_batch_to_kafka(batch):
    if batch:
        payload = json.dumps({"events": batch})
        producer.produce(kafka_topic, value=payload, callback=delivery_report)
        producer.flush()
        batch.clear()
        

#############################################################

if __name__ == "__main__":

    with open("config.json","r") as f:
        config = json.load(f)
    kafka_user = config["kafka_user"]
    kafka_api_pass = config["kafka_api_pass"]
    bootstrap_server = config["bootstrap_server"]
    
    kafka_topic = "binance_trades"
    
    
    
    conf = {
        'bootstrap.servers': bootstrap_server,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': kafka_user,
        'sasl.password': kafka_api_pass
    }
    
    producer = Producer(conf)
    
    #trades_count = 0
    batch = []   # to keep the data as batches and send out to kafka
    
    
    # Binance WebSocket endpoint for BTC/USDT trades
    socket = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    
    
    batch = []
    
    
    i = 0
    for i in range(3):
        run_ws(3)      # duration of 5 seconds
        time.sleep(5)


    