# python_mock_kafka_producer

How to use
```bash
#FIRSTLY CREATE A VIRTUAL ENV AFTER CLONING THE PROJECT INSIDE THE PROJECT`s ROOT FOLDER
#THEN EXECUTE
source venv/bin/activate
usage: producer.py [-h] --env ENV [--template TEMPLATE] [--json-source JSON_SOURCE] [--json-source-key-path JSON_SOURCE_KEY_PATH] [--topic TOPIC] [--count COUNT] [--workers WORKERS] [--start START]
                   [--assign ASSIGN]

Producer

options:
  -h, --help            show this help message and exit
  --env ENV, -e ENV     Environment where you wanna produce
  --template TEMPLATE, -t TEMPLATE
                        The template to be produced
  --json-source JSON_SOURCE, -j JSON_SOURCE
                        A file containing an JSON object per line
  --json-source-key-path JSON_SOURCE_KEY_PATH
                        The json path for the key Like: {id:{serial: 1, position:0}, some_value:1} -> id.serial to access serial value you can combine multiple paths like: a.b,a.b.c they will be dash joined
  --topic TOPIC         A topic that will replace the config`s default topic
  --count COUNT, -c COUNT
                        Number of events you wanna produce
  --workers WORKERS, -w WORKERS
                        Number of parallel workers
  --start START, -s START
                        The start offset of the event position
  --assign ASSIGN       You can assign values to your template using python glom query language Like: --assign=a.b='U' to replace {a:{b:'A' to 'U'}}

# assuming that the config file has already an environment called <local> and you already have a template defined, to produce an event it would be

python producer.py -e local

[2023-07-25 15:45:14,331][INFO][__main__] - producing 1 env[local] topic[topic_name_example]
[2023-07-25 15:45:14,448][INFO][Worker-1] - 1 events produced
```
