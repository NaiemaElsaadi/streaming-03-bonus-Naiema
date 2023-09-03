"""
    This program sends a message to a queue on the RabbitMQ server.

    Naiema Elsaadi
    Date: August 30, 2023

"""
import pika
import csv
import signal
import sys


# RabbitMQ connection parameters
connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()

# Declare the queue to consume messages from
queue_name = 'csv_messages'
channel.queue_declare(queue=queue_name)

# Output file path
output_file_path = 'received_messages.csv'

def write_to_file(message):
    with open(output_file_path, 'a') as output_file:
        output_file.write(message + '\n')
        print(f"Received and written: {message}")

def callback(ch, method, properties, body):
    message = body.decode()
    write_to_file(message)

# Set up the consumer
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue=queue_name)
channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
print(' [*] Waiting for messages. To exit press CTRL+C')




# Start consuming
try:
    channel.start_consuming()
except KeyboardInterrupt:
    print("\nConsumer interrupted. Exiting...")
    connection.close()
