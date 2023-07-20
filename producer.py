from pyconfigparser import configparser
from multiprocessing import Process
from kafka.future import Future
from kafka import KafkaProducer
from glom import glom, Assign
from schema import Optional
from os.path import isfile
from random import Random
from typing import List
from faker import Faker
import datetime
import logging
import argparse
import json
import time
import sys
import os

CONFIG_SCHEMA = {
    str: {
        'bootstrap_servers': str,
        'topic': str,
        Optional('queue_size', default=5000): int
    }
}

DEFAULT_WORKER_GLOBALS = {
    '__builtins__': globals()['__builtins__'],
    'datetime': datetime,
    'random': Random(),
    'fake': Faker(),
    'time': time,
    'sys': sys,
    'os': os,
}

logging.basicConfig(format='[%(asctime)s][%(levelname)s][%(name)s] - %(message)s')
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


def get_assignments(assigns: str):
    if assigns is None:
        return []

    assigns = assigns.split(',')

    def get_ass(assign: str):
        assign = assign.strip().split(':')
        if len(assign) != 2 or '' in assign:
            LOG.error(f"{assign} should match 'path=value,path=value'")
            exit(1)

        path, value = assign
        return path, eval(value)

    return list(map(get_ass, assigns))


def current_time_milli():
    return round(time.time() * 1000)


def sync(futures: List[Future]):
    for f in futures:
        if not f.is_done:
            f.get()


def produce(worker_id, producer_config, tmplt: str, assigns: list, position: int, end: int):
    futures = []
    log = logging.getLogger(f'Worker-{worker_id}')
    log.setLevel(logging.INFO)

    producer = KafkaProducer(bootstrap_servers=producer_config.bootstrap_servers)
    topic = producer_config.topic

    start = position
    last_count = position
    last_second = current_time_milli()

    while position < end:
        worker_globals = {**DEFAULT_WORKER_GLOBALS, 'position': position}
        key = f'Worker-{worker_id} Position-{position}'.encode('utf-8')

        tmplt = eval(tmplt, worker_globals)
        if assigns:
            for path, value in assigns:
                tmplt = glom(tmplt, Assign(path, value))
        tmplt = json.dumps(tmplt).encode('utf-8')

        future = producer.send(topic, tmplt, key)
        futures.append(future)
        future.add_errback(
            lambda e: log.error('error while sending the message of position %d - Error msg: %s', position, str(e))
        )

        position += 1

        if len(futures) == producer_config.queue_size:
            log.info('flushing %s future events', producer_config.queue_size)
            sync(futures)
            futures = []

        if (current_time_milli() - last_second) >= 1000:
            delta = position - last_count
            last_count = position
            last_second = current_time_milli()
            log.info("start:%d - end:%d - count:%d - delta:%d/s", start, end, position, delta)

    sync(futures)
    log.info('%s events produced', end - start)


def main():
    parser = argparse.ArgumentParser(description='Producer')
    parser.add_argument('--template', '-t', type=str, default='template', help='The template to be produced')
    parser.add_argument('--env', '-e', type=str, required=True, help='Environment where you wanna produce')
    parser.add_argument('--count', '-c', type=int, default=1, help='Number of events you wanna produce')
    parser.add_argument('--workers', '-w', type=int, default=1, help='Number of parallel workers')
    parser.add_argument('--start', '-s', type=int, default=1, help='The start offset of the event position')
    parser.add_argument('--assign', type=str, help="You can assign values to your\
                            template using python glom query language\n \
                            Like: --assign=a.b='U' to replace {a:{b:'A' to 'U'}}")

    args, _ = parser.parse_known_args()

    config = configparser.get_config(CONFIG_SCHEMA, config_dir='')

    if args.env not in config.keys():
        LOG.error("'%s' is not present in the config - Available environments: %s", args.env, config.keys())
        exit(1)

    if args.count < args.workers:
        LOG.error('The number of events cannot be smaller than the number of workers')
        exit(1)

    if not isfile(args.template):
        LOG.error(
            'Create a default template file or create a template file and pass it using --template/-t argument')
        exit(1)

    kafka_config = config[args.env]

    assigns = get_assignments(args.assign)

    workers = args.workers
    count = round(args.count / workers)

    LOG.info('Producing %s on env %s', args.count, args.env)
    with open(args.template) as template:
        template = template.read()
        start = args.start

        for worker_id in range(1, workers + 1):
            worker_args = (worker_id, kafka_config, template, assigns, start, start + count)
            Process(target=produce, args=worker_args).start()
            start = start + count


if __name__ == '__main__':
    main()
