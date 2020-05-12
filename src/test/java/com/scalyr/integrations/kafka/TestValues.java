/*
 * Copyright 2020 Scalyr Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.scalyr.integrations.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

/**
 * Values to use for testing
 */
public abstract class TestValues {
  public static final String TOPIC_VALUE = "LogsTopic";
  public static final String MESSAGE_VALUE = "Test log message";
  public static final String LOGFILE_VALUE = "/var/log/syslog";
  public static final String SERVER_VALUE = "server";
  public static final String PARSER_VALUE = "systemLogPST";
  public static final String API_KEY_VALUE = "abc123";
  public static final int ADD_EVENTS_TIMEOUT_MS = 20_000;
  public static final int ADD_EVENTS_RETRY_DELAY_MS = 1000;
  public static final int EXPECTED_NUM_RETRIES = 5; // Expected num of retries with ADD_EVENTS_TIMEOUT_MS and ADD_EVENTS_RETRY_DELAY_MS
  public static final long EXPECTED_SLEEP_TIME_MS = 15_000; // Expected sleep time with ADD_EVENTS_TIMEOUT_MS and ADD_EVENTS_RETRY_DELAY_MS
  public static final String ENRICHMENT_VALUE = "env=test,org=Scalyr";
  public static final Map<String, String> ENRICHMENT_VALUE_MAP = TestUtils.makeMap("env", "test", "org", "Scalyr");
  public static final int BATCH_SEND_SIZE_BYTES = MESSAGE_VALUE.length();
  public static final String ADD_EVENTS_RESPONSE_SUCCESS;
  public static final String ADD_EVENTS_RESPONSE_SERVER_BUSY;
  public static final String ADD_EVENTS_RESPONSE_CLIENT_BAD_PARAM;

  static {
    // AddEventsResponse response messages
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      ADD_EVENTS_RESPONSE_SUCCESS = objectMapper.writeValueAsString(new AddEventsClient.AddEventsResponse()
        .setStatus(AddEventsClient.AddEventsResponse.SUCCESS).setMessage(AddEventsClient.AddEventsResponse.SUCCESS));
      ADD_EVENTS_RESPONSE_SERVER_BUSY = objectMapper.writeValueAsString(new AddEventsClient.AddEventsResponse()
        .setStatus("error/server/busy").setMessage("Requests are throttled.  Try again later"));
      ADD_EVENTS_RESPONSE_CLIENT_BAD_PARAM = objectMapper.writeValueAsString(new AddEventsClient.AddEventsResponse()
        .setStatus("error/client/badParam").setMessage("Maybe caused by bad api key"));

    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

}
