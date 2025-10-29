import json

import pika


class Messenger:
    def __init__(self, conn: pika.connection.Connection, ch: pika.channel.Channel, name: str):
        self.name = name
        self.conn = conn
        self.ch = ch

    def check_channel_open(self):
        if not self.ch.is_open:
            raise ConnectionError("Channel is closed.")


    def send_msg(self, queue: str, data: str):
        self.check_channel_open()
        # TODO why is exchange an empty string?
        self.ch.basic_publish(exchange="", routing_key=queue, body=data)


    def send_stop(self, job_id: str):
        stop = {"status": "STOPPED", "job_id": job_id}
        self.send_msg(f"{self.name} result", json.dumps(stop))


    def send_done(self, job_id: str):
        done = {"status": "SUCCESS", "job_id": job_id}
        self.send_msg(f"{self.name} result", json.dumps(done))


    def ack_msg(self, method: pika.frame.Method):
        self.check_channel_open()
        self.ch.basic_ack(delivery_tag=method.delivery_tag)
