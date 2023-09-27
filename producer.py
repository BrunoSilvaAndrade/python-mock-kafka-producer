from pyconfigparser import configparser
from multiprocessing import Process
from kafka import KafkaProducer
from glom import glom, Assign
from schema import Optional
from os.path import isfile
from random import Random
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
        'kafka': {'bootstrap_servers': str},
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


class ProducerFlusherWrapper:
    def __init__(self, producer, queue_size):
        self.producer = producer
        self.queue_size = queue_size
        self.futures = []

    def produce(self, topic, value=None, key=None):
        future = self.producer.send(topic, value, key)
        self.futures.append(future)

        if len(self.futures) == self.queue_size:
            LOG.info('flushing %s future events', self.queue_size)
            self.flush()
        return future

    def flush(self):
        for f in self.futures:
            if not f.is_done:
                f.get()
        self.futures = []


class ProducerWatcher:
    def __init__(self):
        self.last_count = 0
        self.last_capture = current_time_milli()

    def callback_if_after_a_second(self, next_count, callback):
        if (current_time_milli() - self.last_capture) >= 1000:
            delta = next_count - self.last_count
            self.last_count = next_count
            self.last_capture = current_time_milli()
            callback(delta)


class CheckersHolder:
    def __init__(self):
        self.validations = []

    def add(self, is_invalid, callback):
        tup = is_invalid, callback
        self.validations.append(tup)

    def exit_if_fail(self):
        for is_invalid, callback in self.validations:
            if is_invalid:
                callback()
                exit(1)


def dump_as_json_utf8_encoded(o):
    return json.dumps(o).encode('utf-8')


def current_time_milli():
    return round(time.time() * 1000)


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


def apply_assignments(value, assigns):
    if assigns:
        for p, v in assigns:
            value = glom(value, Assign(p, v))

    return value


def template_producer(args, config, assigns):
    workers = args.workers
    count = 1 if args.count is None else args.count
    count = round(count / workers)

    with open(args.template) as template:
        template = template.read()
        start = args.start

        for worker_id in range(1, workers + 1):
            worker_args = (worker_id, config, template, assigns, start, start + count)
            Process(target=_template_producer, args=worker_args).start()
            start = start + count


def _template_producer(worker_id, producer_config, template: str, assigns: list, position: int, end: int):
    log = logging.getLogger(f'Worker-{worker_id}')
    log.setLevel(logging.INFO)

    producer = ProducerFlusherWrapper(KafkaProducer(**producer_config.kafka), producer_config.queue_size)
    topic = producer_config.topic
    watcher = ProducerWatcher()

    start = position

    while position < end:
        worker_globals = {**DEFAULT_WORKER_GLOBALS, 'position': position}
        key = f'position-{position}'.encode('utf-8')

        value = eval(template, worker_globals)
        value = apply_assignments(value, assigns)
        value = dump_as_json_utf8_encoded(value)

        future = producer.produce(topic, value, key)
        future.add_errback(
            lambda e: log.error('error while sending the message of position %d - Error msg: %s', position, str(e))
        )

        position += 1

        watcher.callback_if_after_a_second(position,
                                           lambda delta: log.info("start:%d - end:%d - count:%d - delta:%d/s", start, end, position, delta))

    producer.flush()
    log.info('%s events produced', end - start)


def make_json_source_key(value, args, count):
    if not args.json_source_key_path:
        return str(count).encode('utf-8')

    key = []
    for path in args.json_source_key_path.split(','):
        path = path.strip()
        data = glom(value, path)

        if type(data) is list or type(data) is dict:
            key.append(json.dumps(data))
        else:
            key.append(str(data))

    return '-'.join(key).encode('utf-8')


def json_source_producer(args, producer_config, assigns):
    log = logging.getLogger('Json Source Producer')
    producer = ProducerFlusherWrapper(KafkaProducer(**producer_config.kafka), producer_config.queue_size)
    topic = producer_config.topic
    watcher = ProducerWatcher()

    with open(args.json_source) as source:
        position = 0
        for line in source:
            value = json.loads(line)
            value = apply_assignments(value, assigns)
            key = make_json_source_key(value, args, position)
            value = dump_as_json_utf8_encoded(value)

            future = producer.produce(topic, value, key)
            future.add_errback(
                lambda e: log.error('error while sending the message of position %d - Error msg: %s', position, str(e))
            )

            position += 1

            watcher.callback_if_after_a_second(position,
                                               lambda delta: log.info("count: %d, delta:%s/s", position, delta))

            if args.count is not None and args.count <= position:
                break

        producer.flush()


def main():
    parser = argparse.ArgumentParser(description='Producer')
    parser.add_argument('--env', '-e', type=str, required=True, help='Environment where you wanna produce')
    parser.add_argument('--template', '-t', type=str, help='The template to be produced')
    parser.add_argument('--json-source', '-j', type=str, help='A file containing an JSON object per line')
    parser.add_argument('--json-source-key-path', type=str, help='The json path for the key\n \
                            Like: {id:{serial: 1, position:0}, some_value:1} -> id.serial to access serial value\n \
                            you can combine multiple paths like: a.b,a.b.c they will be dash joined')
    parser.add_argument('--topic', type=str, help='A topic that will replace the config`s default topic')
    parser.add_argument('--count', '-c', type=int, help='Number of events you wanna produce')
    parser.add_argument('--workers', '-w', type=int, default=1, help='Number of parallel workers')
    parser.add_argument('--start', '-s', type=int, default=1, help='The start offset of the event position')
    parser.add_argument('--assign', type=str, help="You can assign values to your\
                            template using python glom query language\n \
                            Like: --assign=a.b='U' to replace {a:{b:'A' to 'U'}}")

    args, _ = parser.parse_known_args()
    config = configparser.get_config(CONFIG_SCHEMA, config_dir='')
    checker = CheckersHolder()

    checker.add(args.env not in config.keys(),
                lambda: LOG.error("'%s' is not present in the config - Available envs: %s", args.env, config.keys()))

    checker.add(args.count is not None and args.count < 1,
                lambda: LOG.error("The count number must be a positive number bigger than 0"))

    checker.add(args.count is not None and args.count < args.workers,
                lambda: LOG.error('The number of events cannot be smaller than the number of workers'))

    checker.add(args.json_source and args.template,
                lambda: LOG.error("You cannot use json-source with template"))

    checker.add(not args.json_source and not args.template,
                lambda: LOG.error("You need to chose a template or provide a json source, check the helper"))

    checker.add(args.template and not isfile(args.template),
                lambda: LOG.error("Template file [%s] not found", args.template))

    checker.add(args.json_source and not isfile(args.json_source),
                lambda: LOG.error("Json source file [%s] not found", args.json_source))

    checker.exit_if_fail()
    config = config[args.env]

    if args.topic is not None:
        config.topic = args.topic

    assigns = get_assignments(args.assign)

    if args.template:
        template_producer(args, config, assigns)

    elif args.json_source:
        json_source_producer(args, config, assigns)


if __name__ == '__main__':
    main()
