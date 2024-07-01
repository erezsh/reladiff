from google.cloud import bigquery

client = bigquery.Client()

table_id = "reladiff-dev-2.reladiff.tmp_rating"
dataset_name = "reladiff"

client.create_dataset(dataset_name, exists_ok=True)

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
)

with open("ratings.csv", "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)

job.result()  # Waits for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id))
