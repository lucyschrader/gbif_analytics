import os
import yaml
import csv
try:
	from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
	from yaml import Loader, Dumper
from jinja2 import Environment, PackageLoader, select_autoescape
from util.config import read_config
from data.io_interface import read_json_file

from playwright.sync_api import sync_playwright as pw

env = Environment(loader=PackageLoader("gbif_analytics"),
                  autoescape=select_autoescape())
save_file = "data/report_data/report_data.yaml"


def load_analytics_data():
	report_mode = read_config("report_mode")
	export_counts, total_records_count = read_export_counts()
	analytics_data = get_analytics_data()

	return analytics_data, export_counts, total_records_count


def read_export_counts():
	export_counts = {}
	total_records_count = 0
	for ex in os.listdir("data/export_counts"):
		with open("data/export_counts/{}".format(ex), "r", encoding="utf-8") as f:
			count_data = yaml.load(f, Loader=Loader)
			year = count_data["year"]
			if not export_counts.get(year):
				export_counts[year] = {}
			month = count_data["month"]
			export_counts[year][month] = count_data

			total_records_count = count_data["records_written"]["core"]

	return export_counts, total_records_count


def get_export_stats():
	update_file = "data/report_data/newexportstats.yaml"
	export_data = {"recordCounts": {}, "additions": {}, "updates": {}}
	with open(update_file, "r", encoding="utf-8") as f:
		update_data = yaml.load(f, Loader=Loader)
		export_data["recordCounts"] = update_data["records_written"]
		export_data["additions"] = update_data["new_record_counts"]
		export_data["updates"] = update_data["update_counts"]
		export_year = update_data["year"]
		export_month = update_data["month"]

	# TODO: Only update file if this data is missing
	with open("data/report_data/exportstatsmonthly.yaml", "w", encoding="utf-8") as f:
		saved_export_data = yaml.load(f, Loader=Loader)
		saved_export_data[export_year][export_month] = export_data
		yaml.dump(export_data, f)

	return saved_export_data


def get_analytics_data():
	analytics_file = "data/report_data/report_data.yaml"
	analytics_data = None
	try:
		with open(analytics_file, "r", encoding="utf-8") as f:
			analytics_data = yaml.load(f, Loader=Loader)
	except IOError:
		print("No saved data found")

	return analytics_data


def load_saved_data():
	if os.path.exists(save_file):
		with open(save_file, "r", encoding="utf-8") as f:
			saved_data = yaml.load(f, Loader=Loader)
			return saved_data
	else:
		return {}


def save_updated_data(data):
	with open(save_file, "w+", encoding="utf-8") as f:
		f.seek(0)
		yaml.dump(data, f, Dumper=Dumper)
		f.truncate()


def build_report_blocks(analytics_data, export_counts, total_records_count):
	# TODO: Probably some fixing up here I dunno
	monthly_counts, mc_columns = build_monthly_counts_data(export_counts)

	print("Building record counts")
	counts_block = build_record_counts_block(monthly_counts, mc_columns)
	write_to_csv("monthlycounts", monthly_counts, mc_columns)

	print("Building total counts")
	totals_block = build_total_counts_block(total_records_count)

	print("Building citations")
	citations_data = read_json_file("data/saved_data/citations.json")
	write_citations(citations_data)
	citations = build_citations_block(citations_data)

	print("Building downloads")
	downloads = build_downloads_block(analytics_data["downloads"])

	print("Building strengths")
	strengths = build_strengths_block(analytics_data["strengths"])

	return counts_block, totals_block, citations, downloads, strengths


def build_monthly_counts_data(data):
	download_stats = load_activity_by_month()
	columns = []
	counts_table = {"Activity by month": [],
	                "Total records uploaded": [],
	                "Total images uploaded": [],
	                "New specimen records": [],
	                "New people records": [],
	                "New taxa records": [],
	                "Updated specimen records": [],
	                "Updated people records": [],
	                "Updated taxa records": []}
	for year in data.keys():
		for month in data[year].keys():
			md = data[year][month]
			date_string = str(year) + str(month)
			columns.append(date_string)

			activity_count_list = [i["count"] for i in download_stats if (i["year"] == year) and (i["month"] == month)]
			if activity_count_list:
				activity_count = activity_count_list[0]
			else:
				activity_count = None

			counts_table["Activity by month"].append({"date": date_string,
			                                          "value": activity_count})
			counts_table["Total records uploaded"].append({"date": date_string,
			                                               "value": md["records_written"].get("core")})
			counts_table["Total images uploaded"].append({"date": date_string,
			                                              "value": md["records_written"].get("multimedia")})

			new_record_counts = md.get("new_record_counts")
			if new_record_counts:
				new_records = new_record_counts.get("object")
				new_people = new_record_counts.get("agent")
				new_taxa = new_record_counts.get("taxon")
			else:
				new_records = None
				new_people = None
				new_taxa = None

			counts_table["New specimen records"].append({"date": date_string,
			                                             "value": new_records})
			counts_table["New people records"].append({"date": date_string,
			                                           "value": new_people})
			counts_table["New taxa records"].append({"date": date_string,
			                                         "value": new_taxa})

			updated_record_counts = md.get("update_counts")
			if updated_record_counts:
				updated_records = updated_record_counts.get("object")
				updated_people = updated_record_counts.get("agent")
				updated_taxa = updated_record_counts.get("taxon")
			else:
				updated_records = None
				updated_people = None
				updated_taxa = None

			counts_table["Updated specimen records"].append({"date": date_string,
			                                                 "value": updated_records})
			counts_table["Updated people records"].append({"date": date_string,
			                                               "value": updated_people})
			counts_table["Updated taxa records"].append({"date": date_string,
			                                             "value": updated_taxa})

	return counts_table, columns


def load_activity_by_month():
	download_counts = []
	with open("data/saved_data/downloads_statistics.tsv", "r", encoding="utf-8") as f:
		reader = csv.DictReader(f, delimiter="\t")
		for row in reader:
			download_counts.append({"year": int(row["year"]),
			                        "month": int(row["month"]),
			                        "count": int(row["number_downloads"])})

	return download_counts


def write_to_csv(label, data, headers):
	headers.insert(0, "")
	with open("report/report_outputs/{}.csv".format(label), "w+", newline="", encoding="utf-8") as outfile:
		writer = csv.writer(outfile, delimiter=",")
		writer.writerow(headers)
		headers.pop(0)
		for key in data.keys():
			write_values = [key]
			for datapoint in data[key]:
				value = datapoint.get("value")
				if not value:
					value = ""
				write_values.append(value)
			writer.writerow(write_values)


def build_total_counts_block(total_records_count):
	print(total_records_count)
	metadata = read_json_file("data/saved_data/saved_metadata.json")
	try:
		download_count = metadata["total_count"]
		count_template = env.get_template("newrecords.html")
		count_html = count_template.render(total_record_count=total_records_count,
	                                       download_count=download_count)
		return count_html
	except KeyError:
		return "<div class='reportblock content'></div>"


def build_record_counts_block(data, columns):
	table = create_count_table(data, columns)
	count_template = env.get_template("count.html")
	count_html = count_template.render(table=table)

	return count_html


def create_count_table(data, columns):
	rows = []
	for field in data.keys():
		values_dict = {"label": field,
		               "values": []}
		for datapoint in data[field]:
			value = datapoint.get("value")
			if not value:
				value = ""
			values_dict["values"].append(value)
		rows.append(values_dict)

	table_template = env.get_template("counttable.html")
	table_html = table_template.render(columns=columns,
	                                   rows=rows)

	return table_html


def build_citations_block(cite_data):
	cite_count = cite_data["count"]
	latest_citation = cite_data["publications"][0]
	citation_template = env.get_template("citation.html")
	citation_html = citation_template.render(cite_count=cite_count,
	                                         latest_citation=latest_citation)
	return citation_html


def write_citations(data):
	headers = ["reference", "link"]
	print(len(data["publications"]))
	print(data["publications"])

	with open("report/report_outputs/citations.csv", "w+", encoding="utf-8") as f:
		writer = csv.writer(f, delimiter=",")
		writer.writerow(headers)
		for citation in data["publications"]:
			values = []
			ref = citation.get("reference")
			if not ref:
				ref = ""
			values.append(ref)
			link = citation.get("link")
			if not link:
				link = ""
			values.append(link)
			writer.writerow(values)


def build_downloads_block(download_data):
	if read_config("report_mode") == "full":
		downloads_header = "Biggest contributions"
	else:
		downloads_header = "Biggest contributions this month"
	download_template = env.get_template("download.html")
	download_html = download_template.render(downloads=download_data,
	                                         downloads_header=downloads_header)
	return download_html


def build_strengths_block(strengths_data):
	if read_config("report_mode") == "full":
		strengths_header = "Dataset strengths"
	else:
		strengths_header = "Dataset strengths this month"
	strengths_template = env.get_template("strengths.html")
	taxa_data = strengths_data["taxa"]
	loc_data = strengths_data["locations"]
	strengths_html = strengths_template.render(taxa=taxa_data,
	                                           locations=loc_data,
	                                           strengths_header=strengths_header)
	return strengths_html


def combine_blocks(counts_block, totals_block, citations, downloads, strengths):
	print("Building main")
	main_css = "file://{}".format(os.path.abspath("templates/print.css"))
	main_template = env.get_template("main.html")
	main_html = main_template.render(total_counts=totals_block,
	                                 citations=citations,
	                                 downloads=downloads,
	                                 strengths=strengths,
	                                 record_counts=counts_block,
	                                 printcss=main_css)
	return main_html


def create_pdf(html):
	html_file = "report/report_outputs/analyticspage.html"
	with open(html_file, "w+", encoding="utf-8") as outfile:
		outfile.write(html)

	with pw() as p:
		browser = p.chromium.launch()
		page = browser.new_page()
		page.goto("file://{}".format(os.path.abspath(html_file)))
		page.emulate_media(media="screen")
		page.pdf(path="report/report_outputs/analyticspdf.pdf", landscape=True, scale=0.9)


def run():
	analytics_data, export_counts, total_records_count = load_analytics_data()
	counts_block, totals_block, citations, downloads, strengths = build_report_blocks(analytics_data,
	                                                                                  export_counts,
	                                                                                  total_records_count)
	full_html = combine_blocks(counts_block, totals_block, citations, downloads, strengths)
	create_pdf(full_html)
	# TODO: Create CSV output for PowerBI report


if __name__ == "__main__":
	run()
