from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from os.path import basename, dirname
import json
import requests
import logging
import argparse
from google.auth import jwt
import re
from requests.auth import HTTPBasicAuth

PARAM_BUCKET_ID_FIELD = "bucketId"
PARAM_OBJECT_ID_FIELD = "objectId"
PARAM_EVENT_TYPE_FIELD = "eventType"
GCS_OBJECT_CHANGE_EVENT_TYPE = "OBJECT_FINALIZE"
DAG_CONF_BUCKET_ID_FIELD = "bucket_id"
DAG_CONF_OBJECT_ID_FIELD = "object_id"
DAG_CONF_FILENAME_FIELD = "filename"
DAG_CONF_OBJECT_PATH_FIELD = "object_path"
DAG_CONF_FIELD = "conf"
AIRFLOW_RUN_ID_FIELD = "dag_run_id"
SECRET_PATH = "/vault/secrets/gcp_credentials.json"
AIRFLOW_SECRET_PATH = "/vault/secrets/airflow-credentials.json"

AIRFLOW_POST_HEADERS = {"Content-Type": "application/json", "Cache-Control": "no-cache"}
STABLE_AIRFLOW_POST_HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}

AIRFLOW_DAG_TRIGGER_URL_TEMPLATE = "{}/api/experimental/dags/{}/dag_runs"
STABLE_AIRFLOW_DAG_TRIGGER_URL_TEMPLATE = "{}/api/v1/dags/{}/dagRuns"

AIRFLOW_STATUS_URL_TEMPLATE = "{}/api/experimental/test"
STABLE_AIRFLOW_STATUS_URL_TEMPLATE = "{}/api/v1/health"

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
logger.addHandler(handler)


def check_pubsub_object(pubsub_object_id, args):
    # if args.object_string_pattern is left out as an argument in the container, will go with default regex pattern
    if args.object_string_pattern:
        regex_object_pattern = re.compile(args.object_string_pattern)
    else:
        r_pattern = r"(?:([a-zA-Z0-9\-_]+\/)*(([a-zA-Z0-9\-_]+\/)|[a-zA-Z0-9\-_]+\.(txt|zip|csv|jpe?g|parquet|xlsx?|pkl|pdf|docx?)))"
        regex_object_pattern = re.compile(r_pattern)

    if re.match(regex_object_pattern, pubsub_object_id) is not None:
        logging.info(f"match changed object found: {pubsub_object_id}")
        return pubsub_object_id
    elif re.search(regex_object_pattern, pubsub_object_id) is not None:
        logging.info(f"search changed object found: {pubsub_object_id}")
        return pubsub_object_id
    elif re.fullmatch(regex_object_pattern, pubsub_object_id) is not None:
        logging.info(f"full match changed object found: {pubsub_object_id}")
        return pubsub_object_id
    else:
        logging.info("nothing changed in bucket paths...")


def regex_dag_caller(pubsub_object_id, args): # airflow_url, run_stable_airflow, regex_json_string
    json_object = json.loads(args.regex_json_string)
    regex_patterns = json_object["pubsub_conf"]
    for r_pattern in regex_patterns:
        bucket_id = r_pattern["bucket_id"]
        dag_id = r_pattern["dag_id"]
        regex_pattern = r_pattern["regex_pattern"]
        object_change = check_pubsub_object(pubsub_object_id, args)
        if object_change:
            run_id = trigger_dag(bucket_id=bucket_id, object_id=object_change, dag_id=dag_id, airflow_url=args.airflow_url, run_stable_airflow=args.run_stable_airflow)
            if run_id:
                logging.info("run id: {} for the dag: {}".format(run_id, dag_id))
                return True
            else:
                logging.info("Dag trigger was unsuccessful using regex_json_string")
                return False


def trigger_dag(bucket_id, object_id, dag_id, airflow_url, run_stable_airflow):
    params = {
        DAG_CONF_BUCKET_ID_FIELD: bucket_id,
        DAG_CONF_OBJECT_ID_FIELD: object_id,
        DAG_CONF_FILENAME_FIELD: basename(object_id),
        DAG_CONF_OBJECT_PATH_FIELD: dirname(object_id)
        }
    with open(AIRFLOW_SECRET_PATH) as f:
            airflow_credentials = json.load(f)

    # key = airflow_credentials["key"]
    # creds = json.loads(key)

    username = airflow_credentials['username']
    password = airflow_credentials['password']

    if run_stable_airflow:
        url = STABLE_AIRFLOW_DAG_TRIGGER_URL_TEMPLATE.format(airflow_url, dag_id)
        headers = STABLE_AIRFLOW_POST_HEADERS
        dag_param = {DAG_CONF_FIELD: params}
    else:
        url = AIRFLOW_DAG_TRIGGER_URL_TEMPLATE.format(airflow_url, dag_id)
        headers = AIRFLOW_POST_HEADERS
        dag_param = {DAG_CONF_FIELD: params}

    logging.info(f"airflow url = {url}")
    logging.info(f"airflow REST API dag run trigger headers - {headers}")
    logging.info(f"{params}")
    try:
        airflow_resp = requests.post(
            url, data=json.dumps(dag_param), headers=headers, auth=HTTPBasicAuth(username, password)
        )
        logging.info("Response from Airflow")
        logging.info(airflow_resp)
    except requests.exceptions.RequestException as e:
        logging.error(e)
        return None
    if AIRFLOW_RUN_ID_FIELD in airflow_resp.text:
        try:
            return json.loads(airflow_resp.text)[AIRFLOW_RUN_ID_FIELD]
        except Exception as e:
            print(e)
            print(airflow_resp.text)
    return None


def get_airflow_endpoint_status(args):
    logging.info(f"run_stable_airflow flag = {args.run_stable_airflow}")
    if args.run_stable_airflow:
        url = STABLE_AIRFLOW_STATUS_URL_TEMPLATE.format(args.airflow_url)
    else:
        url = AIRFLOW_STATUS_URL_TEMPLATE.format(args.airflow_url)
    logging.info(f"Airflow Check Status URL = {url}")
    logging.info(url)
    try:
        airflow_resp = requests.get(url)
        logging.info("Get Request sent to airflow status check url")
        logging.info(f"{json.loads(airflow_resp.text)}")
        if args.run_stable_airflow:
            if airflow_resp.text and (json.loads(airflow_resp.text)["metadatabase"]["status"] == "healthy" and json.loads(airflow_resp.text)["scheduler"]["status"] == "healthy"):
                return True
            return False
        else:
            if airflow_resp.text and json.loads(airflow_resp.text)["status"] == "OK":
                return True
            return False
    except requests.exceptions.RequestException as e:
        logging.error(e)
    return False


def handle_message(message, args):
    logging.info("Received message: {}".format(message))
    if message.attributes:
        event_type = message.attributes.get(PARAM_EVENT_TYPE_FIELD)
        if event_type == GCS_OBJECT_CHANGE_EVENT_TYPE:
            bucket_id = message.attributes.get(PARAM_BUCKET_ID_FIELD)
            object_id = message.attributes.get(PARAM_OBJECT_ID_FIELD)
            if args.regex_json_string:
                regex_dag_caller(regex_json_string=args.regex_json_string, pubsub_object_id=object_id, airflow_url=args.airflow_url, run_stable_airflow=args.run_stable_airflow)
            elif args.object_string_pattern:
                object_change_match = check_pubsub_object(pubsub_object_id=object_id, args=args)
                if object_change_match:
                    logging.info(f"object change match found - {object_change_match}")
                    run_id = trigger_dag(bucket_id=bucket_id, object_id=object_change_match, dag_id=args.dag_id, airflow_url=args.airflow_url, run_stable_airflow=args.run_stable_airflow)
                    if run_id:
                        logging.info(f"DAG trigger was successfully triggered with run_id: {run_id} \
                        using object_string_pattern against object changed in pubsub notifications: {object_change_match} ")
                        return True
                    else:
                        logging.info(f"DAG trigger was unsuccessful from pubsub object match - {object_change_match} \
                        or regex_pattern - {args.regex_json_string} as match was not found")
                        raise Exception("DAG Trigger unsuccessful for object match - {}".format(object_change_match))
                else:
                    logging.error("object change match not found in pubsub check using object_id - {}".format(object_id))
            else:
                logging.error(f"DAG failed to trigger using both object_string_pattern argument {args.object_string_pattern} \
                or object_id: {object_id} found in pubsub message attributes.")
                # return False
        else:
            logging.info("Not a Object change event. Ignoring...")
            return True
    else:
        logging.error("Insufficient fields in the message")
        return False


def get_subscriber():
    with open(SECRET_PATH) as fp:
        service_account_info = json.load(fp)
    audience = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"

    credentials = jwt.Credentials.from_service_account_info(
        service_account_info, audience=audience
    )

    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    return subscriber


def create_subscription(args):
    """Create a new pull subscription on the given topic."""
    subscriber = get_subscriber()
    topic_path = subscriber.topic_path(args.project_id, args.topic_id)
    subscription_path = subscriber.subscription_path(args.project_id, args.subscription_id)
    # Wrap the subscriber in a 'with' block to automatically call close() to
    # close the underlying gRPC channel when done.

    with subscriber:
        try:
            subscription = subscriber.create_subscription(subscription_path, topic_path)
            logging.info("Subscription created: {}".format(subscription))
        except:
            logging.info("Subscriptions exists")


def receive_messages(args):
    """Receives messages from a pull subscription."""
    subscriber = get_subscriber()
    # It is expected to throw exception if project or subscription is incorrect
    subscription_path = subscriber.subscription_path(
        args.project_id, args.subscription_id
    )

    def callback(message):
        resp = handle_message(message, args)
        if resp:
            message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    logging.info("Listening for messages on {}..\n".format(subscription_path))

    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.

            streaming_pull_future.result(timeout=args.timeout)
        except TimeoutError:
            streaming_pull_future.cancel()


def get_args():
    parser = argparse.ArgumentParser(description="")

    parser.add_argument(
        "--project_id",
        type=str,
        required=True,
        help="GCP Project id of the subscriber"
    )

    parser.add_argument(
        "--topic_id",
        type=str,
        required=True,
        help="GCP PubSub Topic id to subscribe to",
    )

    parser.add_argument(
        "--subscription_id",
        type=str,
        required=True,
        help="GCP PubSub Subscription id of the subscriber",
    )

    parser.add_argument(
        "--timeout",
        type=float,
        required=False,
        help="Timeout of the subscription receiver in seconds",
        default=600,
    )

    parser.add_argument(
        "--airflow_url",
        type=str,
        required=True,
        help="Airflow URL endpoint used to trigger DAG"
    )

    parser.add_argument(
        "--dag_id",
        type=str,
        required=True,
        help="DAG id of the DAG to be triggered"
    )

    parser.add_argument(
        "--run_stable_airflow",
        action="store_true", default=True,
        help="boolean to toggle Airflow REST api version to use. \
            If flag is called, container will use stable rest API url while if left out of arguments \
            container will use experimental rest api url endpoint"
    )

    parser.add_argument(
        "--object_string_pattern",
        type=str,
        required=False,
        help="Changed object blob path with or without filename, filename as string, or regex pattern \
            to check for `OBJECT_FINALIZE` status in GCS bucket \
            to determine whether to trigger DAG."
    )

    parser.add_argument(
        "--regex_json_string",
        type=str,
        required=False,
        help="regex pattern used to sort multiple object_string_patterns \
            associated with their respective dag_id's to trigger them."
    )

    args = parser.parse_args()
    print(args)
    return args


if __name__ == "__main__":
    args = get_args()
    logging.info(" Parsed Arguments : ")
    logging.info(args)
    status = get_airflow_endpoint_status(args)
    create_subscription(args)
    if status:
        logging.info("Airflow endpoint active")
        receive_messages(args)
    else:
        logging.error("Airflow endpoint inactive")
