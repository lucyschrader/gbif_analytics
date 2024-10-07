from datetime import datetime, date
from collections import Counter
from tqdm import tqdm
from data.datastore import (activity_memo, activity_data, ActivityRecord, taxon_keys, taxon_memo, taxon_data, locations,
                            location_memo)
from data.io_interface import read_directory_files, read_json_file
from util.config import read_config, write_config


def process_activity_data():
	if read_config("report_mode") != "full":
		set_most_recent_month()
	load_required_data(month=read_config("report_mode"))

	count_taxa()
	deduplicate_activity_taxa()
	count_locations()
	deduplicate_locations()


def load_required_data(month=None):
	if month:
		data = read_json_file("data/saved_data/{}-activity.json".format(month))
	else:
		data = read_directory_files("data/saved_data", dontload="saved_metadata.json")

	load_activity_records(data)


def set_most_recent_month():
	now = datetime.now()
	download_m, download_y = (now.month - 1, now.year) if now.month != 1 else (12, now.year - 1)
	most_recent_month = date(download_y, download_m, 1)
	write_config("most_recent_month", most_recent_month)


def load_activity_records(records):
	for record in tqdm(records, desc="Loading records"):
		store_activity_record(record)


def store_activity_record(record):
	activity_key = record["downloadKey"]
	activity_memo[activity_key] = {"taxon_keys": [], "locations": []}
	activity_record = ActivityRecord(activity_key, record)
	post_process(activity_record)
	activity_data.data.append(activity_record)


def post_process(record):
	if read_config("report_mode") == "month":
		check_for_include_in_report(record)
	if record.include_in_report:
		check_for_taxon_details(record)
		check_for_locations(record)
		record.activity_key = record.data["downloadKey"]
		record.doi = record.data["download"].get("doi")
		record.total_records = record.data["download"]["totalRecords"]
		record.te_papa_records = record.data["numberRecords"]
		record.link = "https://www.gbif.org/occurrence/download/{}".format(record.data["downloadKey"])
		check_for_sort_exclusion(record)


def check_for_include_in_report(record):
	most_recent_month = read_config("most_recent_month")
	rec_date = record.data["download"]["created"].split("+")[0]
	rec_datestamp = datetime.fromisoformat(rec_date)
	if rec_datestamp.year == most_recent_month.year and rec_datestamp.month == most_recent_month.month:
		print("Record safe!")
		pass
	else:
		print(rec_datestamp.year, most_recent_month.year)
		print(rec_datestamp.month, most_recent_month.month)
		record.include_in_report = False


def check_for_taxon_details(record):
	try:
		request_predicate = record.data["download"]["request"].get("predicate")
		if request_predicate:
			record_taxon_keys = gather_keys(request_predicate)
			for taxon_key in record_taxon_keys:
				record_taxon_key_use(taxon_key, record.activity_key)
	except KeyError:
		pass


def gather_keys(request_predicate):
	record_taxon_keys = navigate_predicates(request_predicate, "TAXON_KEY")
	return record_taxon_keys


def record_taxon_key_use(taxon_key, activity_key):
	taxon_keys.append(taxon_key)

	activity_memo[activity_key]["taxon_keys"].append(taxon_key)


def check_for_locations(record):
	try:
		request_predicate = record.data["download"]["request"].get("predicate")
		if request_predicate:
			loc_types = ["CONTINENT", "COUNTRY", "STATE_PROVINCE", "LOCALITY"]
			for location_type in loc_types:
				predicate_locations = navigate_predicates(request_predicate, location_type)
				for pred_loc in predicate_locations:
					if not locations.get(location_type):
						locations[location_type] = []
					locations[location_type].append(pred_loc)
					activity_memo[record.activity_key]["locations"].append(pred_loc)

	except KeyError:
		pass


def navigate_predicates(predicate_data, search_key):
	values = []
	for key, value in predicate_data.items():
		if key == "predicates":
			for predicate in value:
				values.extend(navigate_predicates(predicate, search_key))
		elif value == search_key:
			if predicate_data.get("type") == "equals":
				values.append(predicate_data.get("value"))
			elif predicate_data.get("type") == "in":
				values.extend(predicate_data.get("values"))

	return values


def check_for_sort_exclusion(record):
	excluded = False
	if record.te_papa_records == record.total_records:
		excluded = True
	if record.te_papa_records:
		if int(record.te_papa_records) > 245000:
			excluded = True
	excluded_taxa = [5, 6]
	rec_taxon_keys = activity_memo[record.activity_key]["taxon_keys"]
	for taxon in excluded_taxa:
		if taxon in rec_taxon_keys:
			excluded = True

	if excluded:
		record.include_in_report = False


def count_taxa():
	counted_taxa = Counter(taxon_keys)
	for k, v in counted_taxa.items():
		taxon_memo[k] = {"name": None, "count": v}


def deduplicate_activity_taxa():
	for key in activity_memo.keys():
		activity_taxon_keys = activity_memo[key]["taxon_keys"]
		if len(activity_taxon_keys) > 0:
			activity_memo[key]["taxon_keys"] = list(set(activity_taxon_keys))


def count_locations():
	for loc_type in locations.keys():
		counted_locations = Counter(locations[loc_type])
		for k, v in counted_locations.items():
			location_memo[k] = {"location_type": loc_type, "count": v}


def deduplicate_locations():
	for key in activity_memo.keys():
		activity_locations = activity_memo[key]["locations"]
		if len(activity_locations) > 0:
			activity_memo[key]["locations"] = list(set(activity_locations))


def find_greatest_proportion():
	sorted_records = sort_by_contribution()
	return sorted_records


def sort_by_contribution():
	for activity_record in activity_data.data:
		if activity_record.total_records and activity_record.te_papa_records:
			if activity_record.total_records < activity_record.te_papa_records:
				# TODO: Log these oddities
				activity_record.contribution_percentage = 0
			else:
				cont_perc = round((int(activity_record.te_papa_records) / int(activity_record.total_records)) * 100, 2)
				activity_record.contribution_percentage = cont_perc
		else:
			activity_record.contribution_percentage = 0

	sorted_records = sort_activity_records(10)
	return sorted_records


def sort_activity_records(count):
	sorted_records = sorted([i for i in activity_data.data if i.include_in_report],
	                        key=lambda activity_record: activity_record.contribution_percentage,
	                        reverse=True)

	if len(sorted_records) < count:
		range_int = len(sorted_records)
	else:
		range_int = count

	return sorted_records[:range_int]


def get_taxon_record_values(taxon_keys, field_name):
	taxon_data.retrieve_taxon_details(taxon_keys)
	taxon_values = []
	for taxon_key in taxon_keys:
		taxon_record = taxon_data.data[taxon_key]
		lookup_value = taxon_record.data.get(field_name)
		if lookup_value:
			taxon_values.append(lookup_value)
			if field_name == "scientificName":
				taxon_memo[taxon_key]["name"] = lookup_value

	if len(taxon_values) == 1:
		taxon_values = taxon_values[0]

	return taxon_values
