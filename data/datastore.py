from api.api_interface import Request


class ActivityData():
	def __init__(self):
		self.data = []
		self.full_count = 0
		self.month_count = 0


class ActivityRecord():
	def __init__(self, activity_key, data):
		self.activity_key = activity_key
		self.include_in_report = True
		self.taxon_keys = []
		self.locations = []
		self.data = data
		self.doi = None
		self.total_records = None
		self.te_papa_records = None
		self.contribution_percentage = None
		self.main_predicates = {"taxa": None,
		                        "locations": None}
		self.link = None
		self.flattened = False
		self.exclude_from_sort = False


class TaxonRecord():
	def __init__(self, taxon_key):
		self.taxon_key = taxon_key
		self.name = None
		self.data = None

	def query_api_for_taxon(self):
		if not self.data:
			request_kwargs = {"quiet": True,
			                  "sleep": 0.1,
			                  "purpose": "species_lookup",
			                  "method": "GET",
			                  "allow_redirects": True,
			                  "timeout": 5,
			                  "attempts": 3,
			                  "api": "species",
			                  "endpoint": "name",
			                  "usage_key": self.taxon_key}
			taxon_request = Request(**request_kwargs)
			taxon_request.send_query()
			if taxon_request.status_code == 200:
				self.data = taxon_request.response_data

		return self.data


class TaxonData():
	def __init__(self):
		self.data = {}

	def retrieve_taxon_details(self, taxon_keys):
		for taxon_key in taxon_keys:
			taxon_details = self.data.get(taxon_key)
			if not taxon_details:
				self.data[taxon_key] = TaxonRecord(taxon_key)
				self.data[taxon_key].query_api_for_taxon()


class Location():
	def __init__(self, label, location_type):
		self.label = label
		self.location_type = location_type
		self.count = 0


class LocationData():
	def __init__(self):
		self.data = {}


class ActivityStats():
	def __init__(self, datestamp):
		self.datestamp = datestamp
		self.download_count = 0
		self.record_download_count = 0
		self.data_dump_count = 0
		self.taxon_counts = {}
		self.location_counts = {}
		self.citations = []


class DatasetStats():
	def __init__(self, datestamp):
		self.datestamp = datestamp
		self.record_count = 0
		self.new_record_count = 0
		self.modified_record_count = 0
		self.image_count = 0
		self.new_image_count = 0


metadata_memo = {}
activity_memo = {}
activity_data = ActivityData()
location_memo = {}
locations = {}
taxon_keys = []
taxon_memo = {}
taxon_data = TaxonData()
citation_data = None
