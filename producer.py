from pyconfigparser import configparser
from multiprocessing import Process
from kafka import KafkaProducer
from os.path import isfile
import logging
import argparse
import json
import time

CONFIG_SCHEMA = {
    str: {
        'bootstrap_servers': str,
        'topic': str
    }
}


def current_time_milli():
    return round(time.time() * 1000)


def produce(_id, producer_config, tmplt: str, position: int, end: int):
    log = logging.getLogger(f'Worker-{_id}')
    log.setLevel(logging.INFO)

    producer = KafkaProducer(bootstrap_servers=producer_config.bootstrap_servers)
    topic = producer_config.topic

    start = position
    last_count = position
    last_second = current_time_milli()

    while position < end:
        value = eval(tmplt, {**globals(), 'position': position})
        producer.send(topic, json.dumps(value).encode('utf-8'))
        position += 1

        if (current_time_milli() - last_second) >= 1000:
            delta = position - last_count
            last_count = position
            last_second = current_time_milli()
            log.info("start:%d - end:%d - count:%d - delta:%d/s", start, end, position, delta)

    log.info('%s events produced', end - start)


def main():
    logging.basicConfig(format='[%(asctime)s][%(levelname)s][%(name)s] - %(message)s')
    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)

    parser = argparse.ArgumentParser(description='Producer')
    parser.add_argument('--template', '-t', type=str, default='template.py', help='The template to be produced')
    parser.add_argument('--env', '-e', type=str, required=True, help='Environment where you wanna produce')
    parser.add_argument('--count', '-c', type=int, default=1, help='Number of events you wanna produce')
    parser.add_argument('--workers', '-w', type=int, default=1, help='Number of parallel workers')
    parser.add_argument('--start', '-s', type=int, default=1, help='The start offset of the event position')
    args, _ = parser.parse_known_args()

    config = configparser.get_config(CONFIG_SCHEMA, config_dir='')

    if args.env not in config.keys():
        log.error("'%s' is not present in the config - Available environments: %s", args.env, config.keys())
        exit(1)

    if args.count < args.workers:
        log.error('The number of events cannot be smaller than the number of workers')
        exit(1)

    if not isfile(args.template):
        log.error(
            'Create a default template.py file or create a template file and pass it using --template/-t argument')
        exit(1)

    kafka_config = config[args.env]

    workers = args.workers
    count = round(args.count / workers)

    log.info('Producing %s on env %s', args.count, args.env)
    with open(args.template) as template:
        template = template.read()
        start = args.start

        for worker_id in range(1, workers + 1):
            worker_args = (worker_id, kafka_config, template, start, start + count)
            Process(target=produce, args=worker_args).start()
            start = start + count


if __name__ == '__main__':
    main()
