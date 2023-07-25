# python_mock_kafka_producer

How to use
```bash
#FIRSTLY CREATE A VIRTUAL ENV AFTER CLONING THE PROJECT INSIDE THE PROJECT`s ROOT FOLDER
#THEN EXECUTE
source venv/bin/activate
pip install -r requirements.txt
python producer.py --help

usage: producer.py [-h] --env ENV [--template TEMPLATE] [--topic TOPIC] [--count COUNT] [--workers WORKERS] [--start START] [--assign ASSIGN]

Producer

options:
  -h, --help            show this help message and exit
  --env ENV, -e ENV     Environment where you wanna produce
  --template TEMPLATE   The template to be produced
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
