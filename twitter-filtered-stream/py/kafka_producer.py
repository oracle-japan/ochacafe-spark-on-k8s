from confluent_kafka import Producer
import certifi
import socket

producer = None
topic = None

def acked(err, msg):  
    global delivered_records  
    """Delivery report handler called on  
       successful or failed delivery of message """  
    if err is not None:  
        print("Failed to deliver message: {}".format(err))  
    else:  
        print("Produced record to topic {} partition [{}] @ offset {}".format(msg.topic(), msg.partition(), msg.offset()))  


def send(key: str, value: str):
    print("Producing record to {}: {}\t{}".format(topic, key, value))  
    producer.produce(topic, key=key, value=value, on_delivery=acked)  
    # p.poll() serves delivery reports (on_delivery) from previous produce() calls.  
    producer.poll(0)  

def flush():
    producer.flush()

def init(config_file: str, kafka_client_type: str):
    global producer
    global topic

    d = {}
    with open(config_file) as f:
        for line in f.readlines():
            if(line.find('=') >= 0):
                (key, value) = line.split('=', 1)
                d[key.strip()] = value.strip()

    if(kafka_client_type == 'kafka'):
        producer = Producer({
            'bootstrap.servers': d['bootstrap_servers'],
            'client.id' : socket.gethostname()
        })
    elif(kafka_client_type == 'oci-streaming'):
        producer = Producer({
            'bootstrap.servers': d['streaming_server'],
            'security.protocol': 'SASL_SSL',  
            'ssl.ca.location': certifi.where(),
            'sasl.mechanism': 'PLAIN',
            'sasl.username': f'{d["tenant_name"]}/{d["user_name"]}/{d["pool_id"]}',
            'sasl.password': d['auth_token']
        })
    else:
        raise Exception(f'Unknown kafka-client-type: {kafka_client_type}')
    
    topic = d['topic']

def main():
    pass

if __name__ == '__main__':
    main()