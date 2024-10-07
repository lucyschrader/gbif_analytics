from data.download import download_activity, download_citations
from util.config import load_config, read_config
from util.processing import process_activity_data, find_greatest_proportion
from util.export import export_proportion_report, export_report_data
import cProfile
import pstats


# Still to add
# Find more locations and smash together
# Process/output polygons in a way they can be reviewed
# Track exclusions like has coordinate issues
# Find and sort out the weirdnesses

def run_analytics():
	dataset_id = read_config("dataset_id")
	download_mode = read_config("download_mode")
	download_activity(dataset_id, download_mode)
	download_citations(dataset_id)
	process_activity_data()
	sorted_records = find_greatest_proportion()
	export_proportion_report(sorted_records)
	export_report_data()

if __name__ == "__main__":
	load_config("util/config.yaml")
	if read_config("run_profiler"):
		profiler = cProfile.Profile()
		profiler.enable()
		run_analytics()
		profiler.disable()
		profile_stats = pstats.Stats(profiler)
		profile_stats.strip_dirs()
		profile_stats.dump_stats("profiler.prof")
	else:
		run_analytics()
