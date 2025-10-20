import json
import threading
from typing import Callable

import pika

from .messaging import Messenger


class StoppableThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    @property
    def stopped(self):
        return self._stop_event.is_set()


class StoppableJob(StoppableThread):
    def __init__(self, method: pika.frame.Method, conn: pika.connection.Connection, ch: pika.channel.Channel, name: str, body: bytes, job_id: str, job_fn: Callable):
        # TODO add callback for logging
        super().__init__()
        self.name = name
        self.job_id = job_id
        self.job_fn = job_fn
        self.body = body
        self.conn = conn
        self.method = method
        self.messenger = Messenger(conn, ch, name)

    def run(self):
        # TODO I want to simply look over the job function handler
        print(f"Starting job {self.job_id}")
        try:
            for result in self.job_fn(self.body):
                if self.stopped:
                    self.conn.add_callback_threadsafe(lambda: self.messenger.send_stop(self.job_id))
                    self.conn.add_callback_threadsafe(lambda: self.messenger.ack_msg(self.method))
                    print(f"Stopped job {self.job_id}")
                    return

                job_id = result.get("job_id")
                if job_id is None or job_id != self.job_id:
                    result["job_id"] = self.job_id
                result["status"] = "RUNNING"

                self.conn.add_callback_threadsafe(lambda: self.messenger.send_msg(f"{self.name} result", json.dumps(result)))
        except Exception as e:
            err_msg = repr(e)
            self.conn.add_callback_threadsafe(lambda: self.messenger.send_err(err_msg))
            self.conn.add_callback_threadsafe(lambda: self.messenger.ack_msg(self.method))
            raise e

        print(f"Finished job {self.job_id}")
        self.conn.add_callback_threadsafe(lambda: self.messenger.send_done(self.job_id))
        self.conn.add_callback_threadsafe(lambda: self.messenger.send_msg(f"{self.name} stop job", json.dumps({"job_id": self.job_id})))
        self.conn.add_callback_threadsafe(lambda: self.messenger.ack_msg(self.method))
