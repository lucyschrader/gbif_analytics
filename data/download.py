from math import ceil
from datetime import datetime, date
from tqdm import tqdm
import time
from api.api_interface import Request
from data.io_interface import write_json_file
from data.datastore import metadata_memo
from util.config import read_config


def download_activity(dataset_id, download_mode):
	if download_mode:
		if download_mode == "full":
			request_activity(dataset_id)
		else:
			request_activity(dataset_id, since=download_mode)
	else:
		request_activity(dataset_id, count_only=True)

	write_json_file("data/saved_data/saved_metadata.json", metadata_memo)


def request_activity(dataset_id, since=None, count_only=False):
	request_kwargs = {"quiet": True,
	                  "sleep": 0.1,
	                  "purpose": "dataset_activity",
	                  "method": "GET",
	                  "allow_redirects": True,
	                  "timeout": 5,
	                  "attempts": 3,
	                  "api": "occurrence",
	                  "endpoint": None,
	                  "dataset_id": dataset_id}
	activity_count = get_activity_count(request_kwargs)
	metadata_memo["total_count"] = activity_count

	if count_only:
		return activity_count

	if activity_count:
		page_count = ceil(activity_count / read_config("limit"))
		get_activity_pages(page_count, request_kwargs, since)


def get_activity_count(kwargs):
	kwargs["limit"] = 0
	kwargs["offset"] = 0
	activity_count_request = Request(**kwargs)
	activity_count_request.send_query()
	return activity_count_request.record_count


def get_activity_pages(page_count, request_kwargs, since):
	since_datestamp = None
	if since:
		year = since[:4]
		month = since[-2:]
		since_datestamp = date(int(year), int(month), 1)
	activity = []
	for i in tqdm(range(page_count), desc="Getting activity details"):
#	for i in tqdm(range(30), desc="Getting activity details"):
		page_results = get_activity_page(i, request_kwargs)
		activity.extend(page_results)
		if since:
			if not check_activity_dates(page_results, since_datestamp):
				break
		time.sleep(1)

	dump_activity_by_month(activity, since_datestamp)


def check_activity_dates(page_results, since_datestamp):
	for activity in page_results:
		activity_date = activity["download"]["created"].split("T")[0]
		activity_datestamp = date.fromisoformat(activity_date)
		if activity_datestamp >= since_datestamp:
			continue
		else:
			return False
	return True


def get_activity_page(i, kwargs):
	limit = read_config("limit")
	offset = i * limit
	kwargs["limit"] = limit
	kwargs["offset"] = offset
	activity_page_request = Request(**kwargs)
	activity_page_request.send_query()
	if not activity_page_request.error_message:
		if activity_page_request.status_code == 200:
			return activity_page_request.records
	else:
		return None


def dump_activity_by_month(activity, since_datestamp=None):
	now = datetime.now()
	month = now.month
	year = now.year
	activity_batch = []
	for act in activity:
		activity_date = act["download"]["created"].split("T")[0]
		activity_datestamp = date.fromisoformat(activity_date)
		if activity_datestamp.month == month and activity_datestamp.year == year:
			activity_batch.append(act)
		else:
			dump_month(month, year, activity_batch, since_datestamp)
			if since_datestamp:
				if activity_datestamp < since_datestamp:
					break
			month = activity_datestamp.month
			year = activity_datestamp.year
			activity_batch = [act]

	if len(activity_batch) > 0:
		dump_month(month, year, activity_batch, since_datestamp)


def dump_month(month, year, activity_batch, since_datestamp):
	if month < 10:
		month_string = "0" + str(month)
	else:
		month_string = str(month)
	filepath = "data/saved_data/{y}{m}-activity.json".format(y=str(year), m=month_string)
	replace = False
	if since_datestamp:
		if since_datestamp <= date(year=year, month=month, day=1):
			replace = True
	write_json_file(filepath, activity_batch, replace=replace)


def download_citations(dataset_id):
	request_kwargs = {"quiet": True,
	                  "sleep": 0.1,
	                  "purpose": "citation_search",
	                  "method": "GET",
	                  "allow_redirects": True,
	                  "timeout": 5,
	                  "attempts": 3,
	                  "api": "literature",
	                  "endpoint": "search",
	                  "dataset_id": dataset_id}
	citation_request = Request(**request_kwargs)
	citation_request.send_query()
	if citation_request.status_code == 200:
		save_citations(citation_request.records)


def save_citations(citation_data):
	citation_list = {"count": len(citation_data),
	                 "publications": []}
	for citation in citation_data:
		reference = format_reference(citation)
		try:
			link = citation["websites"][0]
		except (KeyError, IndexError):
			link = None
		publication_data = {"reference": reference,
		                    "link": link}
		citation_list["publications"].append(publication_data)

	write_json_file("data/saved_data/citations.json", citation_list)


def format_reference(citation):
	title = citation["title"]
	authors = citation["authors"]
	author_names = []
	for author in authors:
		author_names.append("{f} {l}".format(f=author["firstName"], l=author["lastName"]))
	authors = ", ".join(author_names)
	year = citation["year"]
	source = citation.get("source")
	reference = "{a}, {t}, ({y}) {s}".format(a=authors,
	                                         t=title,
	                                         y=year,
	                                         s=source)
	return reference
