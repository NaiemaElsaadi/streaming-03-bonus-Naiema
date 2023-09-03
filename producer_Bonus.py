"""
    This program sends a message to a queue on the RabbitMQ server.

    Naiema Elsaadi
    Date: August 30, 2023

"""
import csv
import pika
import time
import sys 
import os


# RabbitMQ connection parameters
connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()


# Declare a new queue
queue_name = 'csv_messages'
channel.queue_declare(queue=queue_name)

# CSV file path
csv_file_path = 'Housing.csv'

def send_message(message):
    channel.basic_publish(exchange='', routing_key=queue_name, body=message)
    print(f"[x] Sent: {message}")

def main():
    with open(csv_file_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        
        for row in csv_reader:
            message = ','.join(row)  # Convert CSV row to a message format
            send_message(message)
            time.sleep(3)  # Sleep for 1-3 seconds

    connection.close()
    sys.exit(0)


    
    
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)