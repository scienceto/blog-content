import json
import requests
import datetime
from dotenv import load_dotenv
from google.cloud import bigquery
from google.auth import default
from google.auth.transport.requests import Request
from google.api_core.exceptions import BadRequest
from bigquery_schema_generator.generate_schema import SchemaGenerator

# initialize project variables
scanned_project = "MONITORING_PROJECT_ID"
bq_dataset_project_id = "DATASET_PROJECT_ID"
bq_dataset_id = "metrics_export"
bq_dataset_location = "asia-south1"
bq_table = "timeseries_data"

# list of metrics to be scanned for the project
# custom_metric_name: promql_query (custom_metric_name can be set anything by the user as per convenience, this name will be added in BQ table for querying in data studio)
metrics_list = {
    'COMPUTE_INSTANCE_CPU': 'avg_over_time(compute_googleapis_com:instance_cpu_utilization[5m])[24h:5m]',
    'COMPUTE_INSTANCE_UPTIME': 'delta(compute_googleapis_com:instance_uptime[5m])[24h:5m]',
    'COMPUTE_INSTANCE_CPU_AVG': 'avg_over_time(compute_googleapis_com:instance_cpu_utilization[24h])[24h:24h]',
    'COMPUTE_INSTANCE_CPU_P95': 'quantile_over_time(0.95, compute_googleapis_com:instance_cpu_utilization[24h])[24h:24h]'
}

# initialize credential and oauth token
credentials, project = default(quota_project_id=scanned_project, scopes=["https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.file"])
credentials.refresh(Request())
oauth_token = credentials.token
oauth_header = {
    "Authorization": f"Bearer {oauth_token}"
}
bq_client = bigquery.Client(project=bq_dataset_project_id, credentials=credentials)

# JSON serializer
def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

# convert json to jsonl
def jsonToJsonl (json_object, jsonl_file_object) :
    for entry in json_object :
        # data += json.dumps(entry)
        entry['scanned_project'] = scanned_project
        jsonl_file_object.write(json.dumps(entry, default=json_serial) + '\n')
    print(f"Total {len(json_object)} rows wrtten to {jsonl_file_object.name} for {scanned_project}.")

# generate bigquery schema from the data using bigquery-schema-generator python module
def generateBqTableSchema (jsonl_file_object, schema_file_object, quoted_values_are_strings=True) :
    generator = SchemaGenerator(
        input_format="json",
        keep_nulls=True,
        quoted_values_are_strings=quoted_values_are_strings
    )
    generator.run(input_file=jsonl_file_object, output_file=schema_file_object)
    print(f"Schema generated for {jsonl_file_object.name} for {scanned_project}.")

# delete previous day data from the table
def deletePreviousScanEntries (bq_table, resource) :
    query = f"""DELETE FROM `{bq_dataset_project_id}.{bq_dataset_id}.{bq_table}`
    WHERE scanned_project=\'{scanned_project}\' AND scanned_metric=\'{resource}\'"""
    query_job = bq_client.query(query)
    result = query_job.result()
    return result

# import data to the bigquery table (if the table doesnot exist, it is created automatically as apparent from the job_config)
def importToBqTable (jsonl_file_object, bq_table, schema_file_object, bq_dataset=bq_dataset_id, bq_data_project=bq_dataset_project_id, bq_dataset_location=bq_dataset_location) :
    load_dotenv()
    # Configures the load job to append the data to the destination table,
    # allowing field addition
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.schema_update_options = [
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
    ]
    # In this example, the existing table contains only the 'full_name' column.
    # 'REQUIRED' fields cannot be added to an existing schema, so the
    # additional column must be 'NULLABLE'.
    job_config.schema = json.load(schema_file_object)
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = False
    ########
    dataset_ref = bq_client.dataset(bq_dataset)
    table_ref = dataset_ref.table(bq_table)
    job = bq_client.load_table_from_file(
        jsonl_file_object,
        table_ref,
        location=bq_dataset_location,
        project=bq_data_project,
        job_config=job_config,
    )
    try:
        job.result()
    except BadRequest as e:
        for e in job.errors:
            print(f"{scanned_project} ERROR: {e['message']}")
        return 'BQ_LOAD_FAILED'
    print(f"Loaded {job.output_rows} rows into {bq_dataset}:{bq_table} for {scanned_project}.")
    return 'BQ_LOAD_SUCCESS'

# function entry, fetch monitoring timeseries data using promql query and call respective functions
def main (request) :
    metrics_result_list = []
    url = f"https://monitoring.googleapis.com/v1/projects/{scanned_project}/location/global/prometheus/api/v1/query"
    for key, metric in metrics_list.items() :
        request_body = {
            "query" : metric
        }
        response = requests.post(url, json=request_body, headers=oauth_header)
        if response.status_code == 200 :
            for resource in response.json().get('data', {}).get('result', []) :
                for i in resource['values']:
                    tmp_dict = {}
                    tmp_dict = {
                        'query': metric,
                        'scanned_metric': key,
                        'ts': datetime.datetime.utcfromtimestamp(i[0]).strftime('%Y-%m-%d %H:%M:%S'),
                        'value': i[1]
                    }
                    tmp_dict.update(resource['metric'])
                    metrics_result_list.append(tmp_dict)
        else :
            print(f"Received nonzero response code for metric {metric}.")

    jsonl_file_object = open('/tmp/metrics_result.jsonl', 'w')
    jsonToJsonl(json_object=metrics_result_list, jsonl_file_object=jsonl_file_object)
    jsonl_file_object = open('/tmp/metrics_result.jsonl')
    schema_file_object = open('/tmp/metrics_result_schema.json', 'w')
    generateBqTableSchema(
        jsonl_file_object=jsonl_file_object,
        schema_file_object=schema_file_object,
        quoted_values_are_strings=False
    )
    jsonl_file_object = open('/tmp/metrics_result.jsonl', 'rb')
    schema_file_object = open('/tmp/metrics_result_schema.json')
    for key in metrics_list:
        try :
            deletePreviousScanEntries(bq_table=bq_table, resource=key)
        except :
            pass
    return importToBqTable(jsonl_file_object=jsonl_file_object, bq_table=bq_table, schema_file_object=schema_file_object)
