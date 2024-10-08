from requests import get, exceptions
import time


class Query():
	def __init__(self,
	             method=None,
	             url=None,
	             allow_redirects=True,
	             timeout=5,
	             attempts=3,
	             sleep=0.1,
	             quiet=False):
		self.response = None

		for attempt in range(attempts):
			if not self.response:
				try:
					if not quiet:
						print("Requesting {}".format(url))
					if method == "GET":
						self.response = get(url, timeout=timeout, allow_redirects=allow_redirects)
				except exceptions.Timeout:
					if not quiet:
						print("{} timed out".format(url))
					time.sleep(sleep)
				except exceptions.ConnectionError:
					if not quiet:
						print("Disconnected trying to get {}".format(url))
					time.sleep(sleep)
				except exceptions.HTTPError:
					if not quiet:
						print("Error {e} trying to get {u}".format(e=self.response.status_code, u=url))
					time.sleep(sleep)

		if not self.response:
			print("Query {m} {u} failed".format(m=method, u=url))


class Request():
	def __init__(self, **kwargs):
		# Functional settings
		self.quiet = kwargs.get("quiet")
		self.sleep = kwargs.get("sleep")
		self.purpose = kwargs.get("purpose")

		# Header settings
		self.method = kwargs.get("method")
		self.allow_redirects = kwargs.get("allow_redirects")
		self.timeout = kwargs.get("timeout")
		self.attempts = kwargs.get("attempts")

		# Query elements
		self.base_url = "https://api.gbif.org/v1"
		self.api = kwargs.get("api")
		self.endpoint = kwargs.get("endpoint")
		self.dataset_id = kwargs.get("dataset_id")
		self.usage_key = kwargs.get("usage_key")
		self.limit = kwargs.get("limit")
		self.offset = kwargs.get("offset")
		self.request_url = None
		self.request_body = None

		# Response elements
		self.status_code = None
		self.response = None
		self.response_data = None
		self.error_message = None
		self.record_count = 0
		self.records = []

		self.complete = False

		self.build_url()

	def build_url(self):
		url_parts = [self.base_url, self.api]
		if self.purpose == "dataset_activity":
			url_parts.append("download")
			url_parts.append("dataset")
			url_parts.append(self.dataset_id)
		elif self.purpose == "species_lookup":
			url_parts.append(self.usage_key)
			url_parts.append(self.endpoint)
		elif self.purpose == "citation_search":
			url_parts.append("search")

		self.request_url = "/".join(url_parts)

		self.append_parameters_to_url()

	def append_parameters_to_url(self):
		params = []
		if self.purpose == "dataset_activity":
			params.append("showDownloadDetails=true")
			params.append("limit={}".format(self.limit))
			params.append("offset={}".format(self.offset))
		elif self.purpose == "citation_search":
			params.append("gbifDatasetKey={}".format(self.dataset_id))

		param_string = "&".join(params)
		self.request_url += ("?" + param_string)

	def send_query(self):
		self.response = Query(method="GET",
				              url=self.request_url,
				              allow_redirects=self.allow_redirects,
				              timeout=self.timeout,
				              attempts=self.attempts,
				              sleep=self.sleep,
				              quiet=self.quiet).response

		if self.response:
			self.status_code = self.response.status_code

			if not self.quiet:
				print("Request {u} status {s}".format(u=self.request_url, s=self.status_code))

			self.check_status()
		else:
			self.error_message = "Unable to retrieve query."

	def check_status(self):
		if self.status_code == 200:
			if self.purpose == "dataset_activity":
				self.save_records()
			elif self.purpose == "species_lookup":
				self.save_response()
			elif self.purpose == "citation_search":
				self.save_records()

	def save_records(self):
		response_json = self.response.json()
		if not self.record_count:
			self.record_count = response_json["count"]
		if response_json.get("results"):
			self.records.extend([i for i in response_json["results"]])

	def save_response(self):
		self.response_data = self.response.json()
