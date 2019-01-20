import datetime
import json
import random
import time

import redis
import requests
import threading


base_time = int(time.time() + 30)


def construct_once_task():
    # delay to run
    base_id = 1000000000000000
    random.seed(time.time())
    base_id += random.randint(100000,999999999)
    print("start",base_id)
    def generate_body():
        random.seed(time.time())
        second = random.randint(0, 1)
        to_run_at = base_time + second
        to_run_str = str(to_run_at)
        nonlocal base_id
        base_id += 1

        d =  {
            "ID": str(base_id),
            "Name": "OncePingTask",
            "ToRunAt": to_run_str,
            "ToRunAfter": "10",
            "Timeout": "1",
            "Url": "http://www.baidu.com"
        }
        return json.dumps(d)
    return generate_body

def construct_period_task():
    base_id = 2000000000000000
    random.seed(time.time())
    base_id += random.randint(100000,999999999)
    print("start",base_id)

    def generate_body():
        random.seed(time.time())
        second = random.randint(0, 10)
        to_run_at = base_time + second
        to_run_str = str(to_run_at)
        end_time = base_time + 600
        end_time_str = str(end_time)
        nonlocal base_id
        base_id += 1

        d =  {
            "ID": str(base_id),
            "Name": "PeriodPingTask",
            "ToRunAt": to_run_str,
            "Timeout": "1", 
            "Interval":"60", # 每分钟运行
            "EndTime":end_time_str,
            "Url": "http://www.baidu.com"
        }
        return json.dumps(d)
    return generate_body


def send_task():
    conn = redis.from_url(url="redis://:uestc12345@127.0.0.1:6379",db=4)
    # p = conn.pubsub(conn)
    generator_once = construct_once_task()
    generator_period = construct_period_task()
    for i in range(1):
        conn.publish("remote-task0:messageQ",generator_period())
    for i in range(0):
        conn.publish("remote-task0:messageQ",generator_once())




if __name__ == "__main__":
    # t = threading.Thread(target=test,args=())
    # t.start()
    send_task()
