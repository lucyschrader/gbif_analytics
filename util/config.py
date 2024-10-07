import yaml

config = {}


def load_config(config_file):
	global config
	with open(config_file, "r", encoding="utf-8") as f:
		config = yaml.safe_load(f)


def read_config(key):
	return config.get(key)


def write_config(key, value):
	config[key] = value
	return value
