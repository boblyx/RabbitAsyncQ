import json
import threading
from typing import Callable

import pika

from .job import StoppableJob
from .messaging import Messenger


def parse_json(messenger: Messenger, data: str):
    try:
        return json.loads(data)
    except Exception as e:
        err_msg = f"Incorrect JSON format for the following message:\n{body}"
        messenger.send_err(err_msg)
        e.add_note(err_msg)
        raise e


def get_job_id(messenger: Messenger, data: dict):
    job_id = data.get('job_id')
    if job_id is None:
        err_msg = "No job_id found in the following message:\n{body}"
        messenger.send_err(err_msg)
        raise ValueError(err_msg)
    return job_id


class JobManager:
    def __init__(self, name: str, conn: pika.connection.Connection, job_fn: Callable, result_fn: Callable, exchange_opt={}, channel_opt={}):
        self.name = name
        self.job_fn = job_fn
        self.result_fn = result_fn
        self.conn = conn
        self.ch = conn.channel()
        self.jobs = {}
        self.exchange_opt = exchange_opt
        self.messenger = Messenger(conn, self.ch, name)

        if "queue" in channel_opt or "on_message_callback" in channel_opt:
            raise ValueError("channel_opt should not have 'queue' or 'on_message_callback' keys.")

        input_job_qname = f"{name} input job"
        stop_job_qname = f"{name} stop job"
        result_qname = f"{name} result"
        self.ch.queue_declare(input_job_qname, **channel_opt)
        self.ch.basic_consume(input_job_qname, self.accept_job)
        self.ch.queue_declare(stop_job_qname, **channel_opt)
        self.ch.basic_consume(stop_job_qname, self.stop_job)
        self.ch.queue_declare(result_qname, **channel_opt)
        self.ch.basic_consume(result_qname, self.handle_result)
        if exchange_opt:
            self.ch.exchange_declare(**exchange_opt)
            self.ch.queue_bind(exchange=exchange_opt["exchange"], queue=input_job_qname, routing_key=input_job_qname)
            self.ch.queue_bind(exchange=exchange_opt["exchange"], queue=stop_job_qname, routing_key=stop_job_qname)
            self.ch.queue_bind(exchange=exchange_opt["exchange"], queue=result_qname, routing_key=result_qname)
        self.ch.start_consuming()

    def accept_job(self, ch: pika.channel.Channel, method: pika.frame.Method, properties: pika.spec.BasicProperties, body: bytes):
        job_data = parse_json(self.messenger, body)

        job_id = get_job_id(self.messenger, job_data)

        job_thread = StoppableJob(method, self.conn, ch, self.name, job_data, job_id, self.job_fn)
        self.jobs[job_id] = job_thread
        job_thread.start()


    def handle_result(self, ch: pika.channel.Channel, method: pika.frame.Method, properties: pika.spec.BasicProperties, body: bytes):
        result_data = parse_json(self.messenger, body)

        job_id = get_job_id(self.messenger, result_data)

        self.result_fn(result_data)
        self.messenger.ack_msg(method)

    def stop_job(self, ch: pika.channel.Channel, method: pika.frame.Method, properties: pika.spec.BasicProperties, body: bytes):
        job_data = parse_json(self.messenger, body)

        job_id = get_job_id(self.messenger, job_data)

        try:
            job_thread = self.jobs.get(job_id)
            if job_thread is None:
                err_msg = f"No job found with ID: {job_id}, skipping."
                self.messenger.send_err(err_msg)
                print(err_msg)
            else:
                job_thread.stop()
                job_thread.join()
                print(f"Stopped job with ID: {job_id}.")
                del self.jobs[job_id]

        except Exception as e:
            err_msg = f"Error stopping job {job_id}."
            self.messenger.send_err(err_msg)
            e.add_note(err_msg)
            raise e

        finally:
            self.messenger.ack_msg(method)
