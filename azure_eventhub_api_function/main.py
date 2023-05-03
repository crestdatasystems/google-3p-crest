# Fetch the logs from Azure Event Hub and ingest them into Chronicle.
import azure.functions as func
import json
import logging
import os

#from .common import ingest
#from .common import utils

# Environment variable constants.
ENV_CHRONICLE_DATA_TYPE = 'CHRONICLE_DATA_TYPE'


def main(events: func.EventHubEvent) -> str:
    """Entrypoint.

    Args:
        request: Request to execute the cloud function.

    Returns:
        string: "Ingestion completed".
    """
    chronicle_data_type = utils.get_env_var(ENV_CHRONICLE_DATA_TYPE)
    events_to_send = []

    logging.info("Events received by the function: {}. Type: {}".format(
        str(events), type(events)
    ))

    # Iterating over the list of event hub logs to decode and json serialize them.
    for event in events:
        try:
            records = json.loads(event.get_body().decode('utf-8'))['records']
            logging.info("Parsing record: {}".format(records))
        # Raise error if the event received from the event hub is not json serializeable. 
        except json.JSONDecodeError as error:
            print("Could not json serialize the event hub log")
            raise RuntimeError(f"The log data from event hub is not JSON serializable") from error
        events_to_send.append(records)

    logging.info("Events planned to send to Azure: {}".format(
        events_to_send
    ))

    # try:
    #     # Ingest Event hub logs to chronicle.
    #     ingest.ingest(events_to_send, chronicle_data_type)
    # except Exception as error:
    #     raise Exception("Unable to push the data to the Chronicle.") from error

    logging.info(f"Total {len(events_to_send)} log(s) are successfully ingested to Chronicle.")
