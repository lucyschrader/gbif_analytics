# Collate and classify use of a dataset on GBIF
This (still very scrappy) set of scripts helps you understand how people are using records from a dataset loaded to [GBIF](https://gbif.org).

It gets the activity report for a specified dataset, which shows the queries run that resulted in data being downloaded.

It then processes the data to see what proportion of the downloaded data came from the dataset, and identifies taxa and locations used by the query.

Finally, it outputs a report that shows:
* How many records are in the dataset
* How many times data from the dataset have been cited, and the most recent citation
* The 10 downloads with the highest proportion of records from the dataset
* The 10 taxa and locations most commonly involved in queries that resulted in data being downloaded

The script also includes some parts that use data generated when dataset exports are created by Te Papa, so aren't going to be that useful for others.

To use, add the dataset id and other parameters to `util/config.yaml` and run `gbif_analytics.py`.