# Standard Library
import json


def load_metrics(filename, config):
    with open(filename) as fin:
        for line in fin:
            j = json.loads(line)
            config[j["metric_name"]] = j


config = {}
load_metrics("default_metrics.json", config)
# load_metrics("customize_metrics.json" ,config)
