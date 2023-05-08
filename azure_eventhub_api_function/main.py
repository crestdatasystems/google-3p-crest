# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Fetch the logs from Azure Event Hub and ingest them into Chronicle."""
import json
import logging
from typing import List

import azure.functions as func
from .common import ingest
from .common import utils

# Environment variable constants.
ENV_CHRONICLE_DATA_TYPE = "CHRONICLE_DATA_TYPE"


def main(events: List[func.EventHubEvent]) -> None:
  """Entrypoint.

  Args:
      events: Events from the Azure Event Hub.
  """
  # Fetch environment variables.
  chronicle_data_type = utils.get_env_var(ENV_CHRONICLE_DATA_TYPE)
  logs_to_send = []

  logging.info("**** Received events: {}. Type: {}".format(str(events), type(events)))
  
  # Iterating over the list of EventHub logs to decode and JSON serialize them.
  for event in events:
    try:
      records = json.loads(event.get_body().decode("utf-8"))["records"]
      logging.info("Record to process: {}, type: {}".format(records, type(records)))
    # Raise error if the event received from the Azure EventHub is not JSON
    # serializable.
    except json.JSONDecodeError as error:
      print("Could not JSON serialize the Azure EventHub log.")
      raise RuntimeError(
          "The log data from Azure EventHub is not JSON serializable."
      ) from error

    # Events are nested in the list form in the EventHub log message.
    if isinstance(records, list):
      logging.info("**** In if condition")
      logs_to_send.extend(records)
    else:
      logging.info("**** In else condition")
      logs_to_send.append(records)

  logs_count = len(logs_to_send)
  logging.info("Parsed {} events from Azure EventHub. Sending them to Chronicle.".format(
    logs_count
  ))

  try:
    # Ingest Azure EventHub logs to Chronicle.
    ingest.ingest(logs_to_send, chronicle_data_type)
  except Exception as error:
    raise Exception("Unable to push the data to the Chronicle.") from error

  logging.info(
      "Total %s log(s) are successfully ingested to Chronicle.",
      len(logs_to_send),
  )
