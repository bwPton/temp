import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointStateReady, EndpointStateConfigUpdate
from mlflow.deployments import get_deploy_client
from logging_utils.logger import get_logger
from config.loader import get_config

config = get_config()
log = get_logger("model-serving")

# === Model Serving Utilities ===
def deploy_model_serving_endpoint(spark, model_name, catalog, schema, endpoint_name, host, token):
    client = get_deploy_client("databricks")
    config = {
        "served_models": [{
            "model_name": model_name,
            "model_version": get_latest_model_version(model_name),
            "workload_type": "CPU",
            "workload_size": "Small",
            "scale_to_zero_enabled": "true",
            "environment_vars": {
                "DATABRICKS_HOST": host,
                "DATABRICKS_TOKEN": token,
                "ENABLE_MLFLOW_TRACING": "true"
            }
        }],
        "auto_capture_config": {
            "catalog_name": catalog,
            "schema_name": schema,
            "table_name_prefix": endpoint_name
        }
    }

    try:
        client.get_endpoint(endpoint_name)
        client.update_endpoint(endpoint=endpoint_name, config=config)
    except:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.`{endpoint_name}_payload`")
        client.create_endpoint(name=endpoint_name, config=config)
def get_latest_model_version(model_name):
    client = MlflowClient(registry_uri="databricks-uc")
    versions = [int(mv.version) for mv in client.search_model_versions(f"name='{model_name}'")]
    return max(versions, default=1)

def wait_for_model_serving_endpoint_to_be_ready(endpoint_name):
    w = WorkspaceClient()
    for i in range(400):
        state = w.serving_endpoints.get(endpoint_name).state
        if state.config_update == EndpointStateConfigUpdate.IN_PROGRESS:
            if i % 40 == 0:
                log.info(f"Waiting for model serving endpoint {endpoint_name}. Current state: {state}")
            time.sleep(10)
        elif state.ready == EndpointStateReady.READY:
            log.info("Model serving endpoint is ready.")
            return
    raise Exception("Timeout waiting for model serving endpoint to become ready.")

def send_request_to_endpoint(endpoint_name, data):
    client = get_deploy_client("databricks")
    return client.predict(endpoint=endpoint_name, inputs=data)

def delete_model_serving_endpoint(endpoint_name):
    client = get_deploy_client("databricks")
    client.delete_endpoint(endpoint_name)
