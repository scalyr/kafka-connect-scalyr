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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Values to use for testing
 */
public abstract class TestValues {
  public static final String TOPIC_VALUE = "LogsTopic";
  public static final String MESSAGE_VALUE = "108.67.8.124 - carroll6278 [14/May/2020:01:36:31 +0000] \"PATCH /leverage/enhance/harness HTTP/1.1\" 201 1243"
    + "\"https://www.leadturn-key.info/e-markets/mesh\" \"Mozilla/5.0 (Windows; U; Windows CE) AppleWebKit/535.25.2 (KHTML, like G***o) Version/6.2 Safari/535.25.2\"";
  public static final String LOGFILE_VALUE = "/var/log/syslog";
  public static final String SERVER_VALUE = "server";
  public static final String PARSER_VALUE = "systemLogPST";
  public static final String CUSTOM_APP_NAME = "customApp";
  public static final String CUSTOM_APP_VERSION = "1.5.0";
  public static final List<String> ACTIVITY_TYPE_VALUE = ImmutableList.of("web-access");
  public static final int SEVERITY_VALUE = 3;

  public static final String API_KEY_VALUE = "abc123";
  public static final int ADD_EVENTS_TIMEOUT_MS = 20_000;
  public static final int ADD_EVENTS_RETRY_DELAY_MS = 500;
  public static final int EXPECTED_NUM_RETRIES = 6; // Expected num of retries with ADD_EVENTS_TIMEOUT_MS and ADD_EVENTS_RETRY_DELAY_MS
  public static final long EXPECTED_SLEEP_TIME_MS = 15_500; // Expected sleep time with ADD_EVENTS_TIMEOUT_MS and ADD_EVENTS_RETRY_DELAY_MS
  public static final String ENRICHMENT_VALUE = "env=test,org=Scalyr";
  public static final Map<String, String> ENRICHMENT_VALUE_MAP = TestUtils.makeMap("env", "test", "org", "Scalyr");
  public static final int MIN_BATCH_SEND_SIZE_BYTES = 500_000;
  public static final int MIN_BATCH_EVENTS = (MIN_BATCH_SEND_SIZE_BYTES / MESSAGE_VALUE.length()) + 1;

  public static final String ADD_EVENTS_RESPONSE_SUCCESS;
  public static final String ADD_EVENTS_RESPONSE_SERVER_BUSY;
  public static final String ADD_EVENTS_RESPONSE_CLIENT_BAD_PARAM;
  public static final String ADD_EVENTS_RESPONSE_INPUT_TOO_LONG;
  public static final String CUSTOM_APP_EVENT_MAPPING_JSON;
  public static final String CUSTOM_APP_EVENT_MAPPING_WITH_DELIMITER_JSON;
  public static final String CUSTOM_APP_MATCH_ALL_EVENT_MAPPING_JSON;


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
      ADD_EVENTS_RESPONSE_INPUT_TOO_LONG = objectMapper.writeValueAsString(new AddEventsClient.AddEventsResponse()
        .setStatus("error/client/badParam").setMessage("input too long (maximum 6000000 characters)"));

      CUSTOM_APP_EVENT_MAPPING_JSON = objectMapper.writeValueAsString(Collections.singletonList(createCustomAppEventMapping(".")));
      CUSTOM_APP_EVENT_MAPPING_WITH_DELIMITER_JSON = objectMapper.writeValueAsString(Collections.singletonList(createCustomAppEventMapping("_")));
      final Map<String, Object> customAppEventMappingMatchAll = createCustomAppEventMapping(".");
      customAppEventMappingMatchAll.put("matcher", ImmutableMap.of("matchAll", true));
      CUSTOM_APP_MATCH_ALL_EVENT_MAPPING_JSON = objectMapper.writeValueAsString(Collections.singletonList(customAppEventMappingMatchAll));

    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create test custom app event mapping as data structure that can be serialized to JSON.
   */
  public static Map<String, Object> createCustomAppEventMapping(String delimiter) {
    Map<String, Object> customAppEventMapper = new HashMap<>();
    customAppEventMapper.put("matcher", TestUtils.makeMap(
      "attribute", "application" + delimiter + "name",
      "value", CUSTOM_APP_NAME));

    customAppEventMapper.put("eventMapping", TestUtils.makeMap(
      "message", "message",
      "logfile", "application" + delimiter + "name",
      "serverHost", "host" + delimiter + "hostname",
      "parser", "scalyr" + delimiter + "parser",
      "version", "application" + delimiter + "version",
      "application", "application" + delimiter + "name",
      "failed", "failed",
      "activityType", "activityType"
    ));

    if (!delimiter.equals(".")) {
      customAppEventMapper.put("delimiter", delimiter);
    }

    return customAppEventMapper;
  }

}
