import yaml
from data.datastore import (activity_memo, activity_data, taxon_memo, taxon_data, location_memo)
from util.processing import sort_activity_records, get_taxon_record_values


def export_report_data():
	downloads = export_downloads()
	strengths = export_strengths()
	print(downloads)
	print(strengths)
	with open("data/report_data/report_data.yaml", "w", encoding="utf-8") as f:
		yaml.dump({"downloads": downloads,
		           "strengths": strengths}, f)


def export_downloads():
	sorted_records = sort_activity_records(10)
	download_data = [flatten_record(record) for record in sorted_records]
	return download_data


def flatten_record(record):
	if not record.flattened:
		record.main_predicates["taxa"] = flatten_taxa(record)
		record.main_predicates["locations"] = flatten_locations(record)
		record.flattened = True

	return record


def flatten_taxa(record):
	record_keys = activity_memo[record.activity_key]["taxon_keys"]
	truncate_keys = False
	if len(record_keys) > 5:
		truncate_keys = True
		record_keys = record_keys[:5]
	elif len(record_keys) < 1:
		return None

	taxon_data.retrieve_taxon_details(record_keys)
	flat_taxa = get_taxon_record_values(record_keys, "scientificName")

	if isinstance(flat_taxa, list):
		if truncate_keys:
			flat_taxa.append("and other taxa")
		flat_taxa = ", ".join(flat_taxa)

	return flat_taxa


def flatten_locations(record):
	record_locations = activity_memo[record.activity_key]["locations"]
	for location in record_locations:
		# TODO: Add lookup
		pass
	return ", ".join(record_locations)


def export_strengths():
	strengths = {"taxa": list_taxa_strengths(),
	             "locations": list_location_strengths()}
	return strengths


def list_taxa_strengths():
	sorted_taxa = sorted([{"taxon_key": taxon_key,
	                       "count": taxon_memo[taxon_key]["count"]} for taxon_key in list(taxon_memo.keys())],
	                     key=lambda taxon: taxon["count"],
	                     reverse=True)
	top_taxa = sorted_taxa[:10]
	taxon_strengths = []
	for taxon in top_taxa:
		taxon_strengths.append({"label": get_taxon_record_values([taxon["taxon_key"]], "scientificName"),
		                        "count": taxon["count"]})

	return taxon_strengths


def list_location_strengths():
	sorted_locations = sorted([{"label": location,
	                            "count": location_memo[location]["count"]} for location in list(location_memo.keys())],
	                          key=lambda location: location["count"],
	                          reverse=True)
	top_locations = sorted_locations[:10]
	location_strengths = []
	for location in top_locations:
		location_strengths.append({"label": location["label"],
		                           "count": location["count"]})

	return location_strengths


def list_activity():
	for activity_record in activity_data.data:
		download_key = activity_record.activity_key
		tp_record_count = activity_record.data["numberRecords"]
		download_record_count = activity_record.data["download"]["totalRecords"]
		print("{d} used {n} records, {tp} from Te Papa".format(d=download_key,
		                                                       n=download_record_count,
		                                                       tp=tp_record_count))


def write_activity():
	lines = []
	for activity_record in activity_data.data:
		download_key = activity_record.activity_key
		download_doi = activity_record["download"]["doi"]
		tp_record_count = activity_record["numberRecords"]
		download_record_count = activity_record["download"]["totalRecords"]
		lines.append("{d} ({dk}) used {n} records, {tp} from Te Papa\n".format(d=download_key,
		                                                                       dk=download_doi,
			                                                                   n=download_record_count,
		                                                                       tp=tp_record_count))
	with open("activity_log.txt", "w") as f:
		f.writelines(lines)


def export_proportion_report(records):
	for record in records:
		report_string = format_report_string(record)
		print(report_string)


def format_report_string(record):
	flatten_record(record)
	report_elements = []
	report_elements.append(record.activity_key)
	report_elements.append(record.doi)
	report_elements.append(record.data["download"]["request"].get("type"))
	report_elements.append(record.te_papa_records)
	report_elements.append(record.total_records)
	report_elements.append(record.contribution_percentage)
	report_elements.append(record.main_predicates["taxa"])
	report_elements.append(record.main_predicates["locations"])

	report_elements = [str(i) for i in report_elements]

	return "\t".join(report_elements)
