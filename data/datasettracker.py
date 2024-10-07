import csv
import click


def record_dataset_details(directory):
	record_count, new_record_count, modified_record_count = count_records("{}/core-export.csv".format(directory))
	write_details(record_count, new_record_count, modified_record_count)


def count_records(file):
	comparator_date = "2024-03-01"
	with open(file, newline="", encoding="utf-8") as f:
		core_data = csv.DictReader(f, delimiter=",")
		record_count = len(core_data)
		new_record_count = 0
		modified_record_count = 0
		for row in core_data:
			if compare_dates(row["created"], comparator_date):
				new_record_count += 1
			elif compare_dates(row["modified"], comparator_date):
				modified_record_count += 1
	return record_count, new_record_count, modified_record_count


def compare_dates(record_date, comparator_date):
	from datetime import datetime
	r_datestamp = datetime.strptime(record_date, "%Y-%m-%d").date()
	c_datestamp = datetime.strptime(comparator_date, "%Y-%m-%d").date()
	if r_datestamp > c_datestamp:
		return True
	else:
		return False


def write_details(record_count, new_record_count, modified_record_count):
	pass
