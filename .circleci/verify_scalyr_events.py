# Copyright 2014-2020 Scalyr Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
import requests
import sys
import time
from six.moves.urllib.parse import quote_plus, urlencode

class ScalyrRequest:
    """
    Abstraction to create scalyr API requests
    TODO: This should be moved to a common test library
    """

    def __init__(self, server_address, read_api_key, max_count=1000, start_time=None):
        self._server_address = server_address
        self._read_api_key = read_api_key
        self._max_count = max_count
        self._start_time = start_time

        self._filters = list()

    def add_filter(self, expr):
        expr = quote_plus(expr)
        self._filters.append(expr)

    def build(self):
        params = {
            "maxCount": self._max_count,
            "startTime": self._start_time or time.time(),
            "token": self._read_api_key,
        }

        params_str = urlencode(params)

        filter_fragments_str = "+and+".join(self._filters)

        query = "{0}&filter={1}".format(params_str, filter_fragments_str)

        return query

    def send(self):
        query = self.build()

        protocol = "https://" if not self._server_address.startswith("http") else ""

        full_query = "{0}{1}/api/query?queryType=log&{2}".format(
            protocol, self._server_address, query
        )

        print("Query server: {0}".format(full_query))

        with requests.Session() as session:
            resp = session.get(full_query, verify=False)

        data = resp.json()

        return data

def check_scalyr_events(additionalFilter):
  """
  Check if Kafka Scalyr connector events are in Scalyr with exponential delay retries.
  return true if they are
  """
  scalyr_server = "app.scalyr.com"
  max_events = 5000
  max_tries = 10
  matches = 0
  retry_delay_sec = 1

  # Scalyr log query request
  request = ScalyrRequest(scalyr_server, os.environ['READ_API_KEY'], max_events, "10 min")
  filter = "origin='kafka-connect-build-" + os.environ['CIRCLE_BUILD_NUM'] + "'"
  request.add_filter(filter)
  if additionalFilter is not None:
    request.add_filter(additionalFilter)


  # Query Scalyr events
  count = 0
  while (count < max_tries and matches < max_events):
    # Exponential delay on retries
    if count > 0:
        time.sleep(retry_delay_sec)
        retry_delay_sec *= 2

    result = request.send()
    matches = len(result['matches'])
    count += 1

  print("Query returned {0} Scalyr events".format(matches))
  return matches == max_events

# Main
if __name__ == "__main__":
    filter = sys.argv[1] if len(sys.argv) > 1 else None
    success = check_scalyr_events(filter)
    sys.exit(0 if success else 1)
