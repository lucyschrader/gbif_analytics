import os
import json


def read_json_file(filepath):
	try:
		with open(filepath, "r", encoding="utf-8") as f:
			data = json.load(f)
			return data
	except json.decoder.JSONDecodeError:
		return None


def write_json_file(filepath, content, replace=False):
	if replace:
		try:
			os.remove(filepath)
		except FileNotFoundError:
			pass
	try:
		with open(filepath, "w+", encoding="utf-8") as f:
			json.dump(content, f)
	except IOError:
		print("Failed to write {}".format(filepath))


def read_directory_files(directory, dontload=None):
	data = []
	for file in os.listdir(directory):
		if file == dontload:
			pass
		else:
			file_data = read_json_file("{}/{}".format(directory, file))
			if file_data:
				data.extend(file_data)

	return data
