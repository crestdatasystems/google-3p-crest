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
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

from common import ingest
from common import utils

# Environment variable constants.
ENV_CHRONICLE_DATA_TYPE = "CHRONICLE_DATA_TYPE"
ENV_KEY_VAULT = "KEY_VAULT"
ENV_SECRET_NAME = "SECRET_NAME"
ENV_SECRET_VERSION = "SECRET_VERSION"


def main(events: List[func.EventHubEvent]) -> None:
  """Entrypoint.

  Args:
      events: Events from the Azure Event Hub.
  """
  logging.info("**** Starting of the main script")
  print("$$$$$ Starting of the main script")
  # Fetch environment variables.
  chronicle_data_type = utils.get_env_var(ENV_CHRONICLE_DATA_TYPE)
  key_vault_name = utils.get_env_var(ENV_KEY_VAULT)
  secret_name = utils.get_env_var(ENV_SECRET_NAME)
  secret_version = utils.get_env_var(ENV_SECRET_VERSION)

  logging.info(f"**** Value of Key Vault: {key_vault_name}, Secret Name: {secret_name}, Secret Version: {secret_version}")
  print(f"$$$$ Value of Key Vault: {key_vault_name}, Secret Name: {secret_name}, Secret Version: {secret_version}")
  vault_uri = f"https://{key_vault_name}.vault.azure.net"
  logging.info(f"**** vault_uri = {vault_uri}")
  print(f"$$$$ vault_uri = {vault_uri}")

  credential = DefaultAzureCredential()
  logging.info("**** Creating Secret client..")
  print("$$$$ Creating Secret client..")
  client = SecretClient(vault_url=vault_uri, credential=credential)

  secret_value = client.get_secret(secret_name)
  logging.info(f"**** Secret value without version: {secret_value.value}")
  print("$$$$ Secret value without version: {secret_value.value}")

  secret_version_value = client.get_secret(name=secret_name, version=secret_version)
  logging.info(f"**** Secret value with version: {secret_version_value.value}")
  print("$$$$ Secret value with version: {secret_version_value.value}")

  logging.info("**** Success *****")
  print("$$$$ Success *****")

  # logs_to_send = []
  
  # # Iterating over the list of EventHub logs to decode and JSON serialize them.
  # for event in events:
  #   try:
  #     records = json.loads(event.get_body().decode("utf-8"))["records"]

  #   # Raise error if the event received from the Azure EventHub is not JSON
  #   # serializable.
  #   except json.JSONDecodeError as error:
  #     print("Could not JSON serialize the Azure EventHub log.")
  #     raise RuntimeError(
  #         f"The log data from Azure EventHub is not JSON serializable. Error: {error}"
  #     ) from error

  #   # Events are nested in the list form in the EventHub log message.
  #   # Example: {"records": [event1, event2, event3, ...]}.
  #   if isinstance(records, list):
  #     logs_to_send.extend(records)
  #   else:
  #     logs_to_send.append(records)

  # logs_count = len(logs_to_send)
  # logging.info(f"Parsed {logs_count} events from Azure EventHub. Sending them to Chronicle.")

  # try:
  #   # Ingest Azure EventHub logs to Chronicle.
  #   ingest.ingest(logs_to_send, chronicle_data_type)
  # except Exception as error:
  #   raise Exception(f"Unable to push the data to the Chronicle. Error: {error}") from error

  # logging.info(
  #     "Total %s log(s) are successfully ingested to Chronicle.",
  #     len(logs_to_send),
  # )
