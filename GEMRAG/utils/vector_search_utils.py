import time
from pyspark.sql import SparkSession
from databricks.vector_search.client import VectorSearchClient
from logging_utils.logger import get_logger
from config.loader import get_config

config = get_config()
spark = SparkSession.builder.getOrCreate()

log = get_logger("vector-search-utils")
# === Vector Search Utilities ===

def vs_endpoint_exists(vsc, vs_endpoint_name):
    try:
        return vs_endpoint_name in [e['name'] for e in vsc.list_endpoints().get('endpoints', [])]
    except Exception as e:
        if "REQUEST_LIMIT_EXCEEDED" in str(e):
            log.warning("Assuming endpoint exists (REQUEST_LIMIT_EXCEEDED)")
            return True
        raise e

def create_endpoint(client, endpoint_name, wait=True):
    if vs_endpoint_exists(client, endpoint_name):
        log.info(f"Endpoint {endpoint_name} already exists.")
        return
    client.create_endpoint(name=endpoint_name, endpoint_type="STANDARD")
    if wait:
        wait_for_vs_endpoint_to_be_ready(client, endpoint_name)
    log.info(f"Endpoint {endpoint_name} is ready.")

def create_or_wait_for_endpoint(vsc, vs_endpoint_name):
    if not vs_endpoint_exists(vsc, vs_endpoint_name):
        vsc.create_endpoint(name=vs_endpoint_name, endpoint_type="STANDARD")
    wait_for_vs_endpoint_to_be_ready(vsc, vs_endpoint_name)

def wait_for_vs_endpoint_to_be_ready(vsc, vs_endpoint_name):
    for i in range(360):
        try:
            endpoint = vsc.get_endpoint(vs_endpoint_name)
        except Exception as e:
            if "REQUEST_LIMIT_EXCEEDED" in str(e):
                log.warning("Assuming endpoint ready (REQUEST_LIMIT_EXCEEDED)")
                return
            raise e
        status = endpoint.get("endpoint_status", endpoint.get("status"))["state"].upper()
        if "ONLINE" in status:
            return endpoint
        if i % 20 == 0:
            log.info(f"Waiting for endpoint to be ready... {endpoint}")
        time.sleep(100)
    raise Exception("Timeout waiting for vector search endpoint to become ready.")

def delete_endpoint(vsc, vs_endpoint_name):
    log.info(f"Deleting endpoint {vs_endpoint_name}...")
    try:
        vsc.delete_endpoint(vs_endpoint_name)
        log.info(f"Endpoint {vs_endpoint_name} deleted successfully.")
    except Exception as e:
        log.error(f"Error deleting endpoint: {e}")

def get_index(client, vector_search_endpoint, vector_index):
    if index_exists(client, vector_search_endpoint, vector_index):
        return client.get_index(vector_search_endpoint, vector_index)
    log.info(f"Index {vector_index} does not exist.")
    return None

def index_exists(vsc, vs_endpoint_name, vs_index_name):
    try:
        vsc.get_index(vs_endpoint_name, vs_index_name).describe()
        return True
    except Exception as e:
        if 'RESOURCE_DOES_NOT_EXIST' not in str(e):
            raise e
        return False

def delete_index(vsc, vs_index_name):
    log.info(f"Deleting index {vs_index_name}...")
    try:
        vsc.delete_index(index_name=vs_index_name)
        return True
    except Exception as e:
        if 'RESOURCE_DOES_NOT_EXIST' not in str(e):
            raise e
        return False

def wait_for_index_to_be_ready(vsc, vs_endpoint_name, vs_index_name):
    for i in range(180):
        idx = vsc.get_index(vs_endpoint_name, vs_index_name).describe()
        status_info = idx.get('status', idx.get('index_status', {}))
        status = status_info.get('detailed_state', status_info.get('status', 'UNKNOWN')).upper()
        if "ONLINE" in status:
            return
        if "UNKNOWN" in status:
            log.warning(f"Assuming index ready: {status_info}")
            return
        if i % 40 == 0:
            log.info(f"Waiting for index to be ready... {status_info}")
        time.sleep(10)
    raise Exception("Timeout waiting for index to become ready.")
def get_index(client, vector_search_endpoint, vector_index):
    if index_exists(client, vector_search_endpoint, vector_index):
        return client.get_index(vector_search_endpoint, vector_index)
    log.info(f"Index {vector_index} does not exist.")
    return None


def create_endpoint(client, endpoint_name, wait=True):
    if vs_endpoint_exists(client, endpoint_name):
        log.info(f"Endpoint {endpoint_name} already exists.")
        return
    client.create_endpoint(name=endpoint_name, endpoint_type="STANDARD")
    if wait:
        wait_for_vs_endpoint_to_be_ready(client, endpoint_name)
    log.info(f"Endpoint {endpoint_name} is ready.")


def update_index(client, vector_search_endpoint, vector_index):
    index = get_index(client, vector_search_endpoint, vector_index)
    if index:
        index.sync()


def create_index(client, vector_search_endpoint, index_name, source_table,
                 primary_key, source_column, columns_to_sync, model_endpoint, wait=True):
    spark.sql(f"ALTER TABLE {source_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

    if not index_exists(client, vector_search_endpoint, index_name):
        log.info(f"Creating index {index_name} on endpoint {vector_search_endpoint}...")
        index = client.create_delta_sync_index(
            endpoint_name=vector_search_endpoint,
            index_name=index_name,
            source_table_name=source_table,
            pipeline_type="TRIGGERED",
            columns_to_sync=columns_to_sync,
            primary_key=primary_key,
            embedding_source_column=source_column,
            embedding_model_endpoint_name=model_endpoint,
        )
        if wait:
            wait_for_index_to_be_ready(client, vector_search_endpoint, index_name)
        return index
    else:
        log.info(f"Index {index_name} already exists.")
        return get_index(client, vector_search_endpoint, index_name)

def create_or_update_direct_index(vsc, vs_endpoint_name, vs_index_name, schema, config):
    try:
        vsc.create_direct_access_index(
            endpoint_name=vs_endpoint_name,
            index_name=vs_index_name,
            schema=schema,
            **config
        )
    except Exception as e:
        if 'RESOURCE_ALREADY_EXISTS' not in str(e):
            raise e
    wait_for_index_to_be_ready(vsc, vs_endpoint_name, vs_index_name)
    log.info(f"Index {vs_index_name} is ready.")
