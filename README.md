# PubSub Trigger container

**Container Overview** - Based on the inputs in the parsed arguments, this container will trigger a DAG if there is a new file or file change detected in a GCS bucket path. Example use case could be to trigger a DAG that starts with a task that reads from bucket path only when there is an object change - this consists of new generation of an existing object, also qualifies as copying or rewriting an existing object.

## Prerequisites
1. **DAG**

    The container is used to determine if there's a pubsub notification `OBJECT_FINALIZE` change detected in a configured google gloud storage bucket that has been setup

    Already developed or create one that reads/looks for a file in a bucket path as the input arg. The bucket path is an object_string_pattern passed into the arguments either as a regex pattern or the object blob string path of the bucket. This bucket path ideally would be an input argument to a airflow task that reads the file used at the start of the DAG workflow.

    The first step of the dag_id to user is aiming to trigger should have dynamic arguments declared & passed into it's input and output arguments as such
    ```
    # Dynamic args
    conf_bucket_id = "{{dag_run.conf['bucket_id']}}"
    conf_object_id = "{{dag_run.conf['object_id']}}"
    conf_object_path = "{{dag_run.conf['object_path']}}"
    conf_filename = "{{dag_run.conf['filename']}}"

    arguments=[
        "--input_bucket_name",
        conf_bucket_id,
        "--input_object_path",
        conf_object_path,
        "--input_filename",
        conf_filename,
        "--output_bucket_name",
        PIPELINE_BUCKET_NAME,
        "--output_path",
        OUTPUT_BUCKET_PATH + PUBSUB_RAW_DATA,
        "--output_filename",
        conf_filename,
        ],
    ```

2. **Setup of Terraform GCP resources**
    - NOTE: Please refer to [Terraform syntax documention](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_notification) for guidance on creating resources in gcp

    - Terraform file with the following resources:
        - gcs bucket - to look for the object change
        - gcp pubsub topic - topic name
        - google storage notification with parameters as such (example below):
            - payload_format = "JSON_API_V1"
            - topic = pubsub_topic_name.id
            - event_type = "OBJECT_FINALIZE"
            - depends_on = `pubsub topic` and `gcs bucket` TF resource Ids in a list
    - Example screenshot of Terraform file setup:

![Terraform pubsub example](img/pubsub_example.png?raw=true)

### 3. **Container Argument Setup**
<br>

| Parsed Arguments         |  Type | Required | Default | Description                                                                                                                                                            |
|--------------------------|:-----:|:--------:|:-------:|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `project_id`             |  str  |   True   |   None  | The GCP project_id of the pubsub scriber (i.e. cp-saa-external-sales-`env`)                                                                                            |
| `topic_id`               |  str  |   True   |   None  | GCP PubSub Topic name to subscribe to                                                                                                                                  |
| `subscription_id`        |  str  |   True   |   None  | GCP PubSub subscription_id                                                                                                                                             |
| `timeout`                | float |   False  |   None  | Timeout (in seconds) of the pubsub subscription receiver                                                                                                               |
| `airflow_url`            |  str  |   True   |   None  | Airflow url endpoint used to trigger the DAG.     |
| `dag_id`                 |  str  |   True   |   None  | DAG id of the dag to be triggered that contains the GCS bucket object blob path with an OBJECT_FINALIZE event                                                          |
| `run_stable_airflow` |  bool |   False   |  False  | Boolean flag to pass whether using stable vs experimental airflow REST API endpoint                                                                                    |
| `object_string_pattern`  |  str  |   False  |   None  | Changed object path or filename string pattern or regex pattern to check to determine whether to trigger                                                               |
| `regex_json_string`      |  str  |   False  |   None  | A json string to parse through dag_id and regex_pattern key:value pairs. Used to determine if pubsub object change blobs have match with regex patterns in json string |

---
<br>

Pubsub topic name, GCS bucket, and dag_id should already be created and already exist in GCP and airflow environments respectively. They will also need be declared or and assigned to variables and passed into the container arguments if required.

### `object_string_pattern`
<br>

* `object_string_pattern` can be passed as custom regex pattern created by the user to capture the object blob or filename string or as the actual string pattern of the object blob path. **Argument should always be passed as a raw literal string**

* `object_string_pattern` as a blob filepath can be passed to the container arguments in several formats/conventions. For example if the GCS bucket URI is the following:
    ```sh
    gs://bucket-example-name/blob_path_example_1/blob_path_example_2/blob_filename_example.csv
    ```
    
    Below are the following ways this argparse argument can be passed into the container:
    ```python
    r"blob_path_example_1/"
    r"blob_path_example_1/blob_path_example_2"
    r"blob_path_example_1/blob_path_example_2/"
    r"blob_path_example_1/blob_path_example_2/blob_filename_example.csv"
    r"blob_filename_example.csv"
    ```

### `regex_json_string`
<br>

* If a no value is passed to `object_string_pattern` container argument, a default large group catching regex pattern is passed into the container as default.
* If custom regex pattern is desired, argument must passed in as a raw literal string format as such - `r"(?:([A-Za-z0-9\/]+))"`
* [Regex Generator](https://regex-generator.olafneumann.org/) is useful resource that will provide a starting point/template for a regex expression based on a sample string. Providing the gcs object path will generate a templated expression specific to capture patterns in the string you require.

* `regex_json_string` can be passed into the container arguments within the DAG file to trigger multiple dags with static dag_ids, bucket_ids & object blob regex_patterns. Regex string patterns within the json string should be captured as raw literal string patterns to best capture any object blob changes within the bucket.
    
In this example, `regex_json_string` would be `regex_lookup_json_string` variable passed as the argument into the parent DAG.

```python
regex_lookup_json_string = '''
{
    "pubsub_conf": [{
    "dag_id": "example_dag_id_1", "regex_pattern": "([a-zA-Z]+(_[a-zA-Z]+)+).*/([a-zA-Z]+(_[a-zA-Z]+)+).*[a-zA-Z]+", "bucket_id": "bucket_name_example_1"},
    {"dag_id": "example_dag_id_2", "regex_pattern": "([a-zA-Z]+(_[a-zA-Z]+)+).*\.[a-zA-Z]+", "bucket_id": "bucket_name_example_2"}
    ]
}
'''
```

**NOTE**: When passing arguments `object_string_pattern` **or** `regex_json_string` into the DAG, not both. If both are passed into the container, the dag_id(s) within the regex_json_string will only be triggered and the object_string_pattern will be ignored.


More information on pubsub functionality, conceptual design, and use cases can be found in the GCP [docs](https://cloud.google.com/pubsub/docs/pull) and [code samples](https://cloud.google.com/pubsub/docs/samples)


Regex patterns can be created using the following resources:
* [Regex 101](https://regex101.com/)
* [Regex Expression](https://regexr.com/)
---