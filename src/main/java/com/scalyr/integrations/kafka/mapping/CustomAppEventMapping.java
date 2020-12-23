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

package com.scalyr.integrations.kafka.mapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Data abstraction to represent a custom app fields -> event attribute mapping.

 Custom application fields may be nested.  Scalyr events support a flat key/value structure.
 Nested Kakfa message fields must be mapped to event attributes.

 Example custom app event mapping:
```
 {
   "matcher": {
      "attribute": "app.name",
      "value": "myapp1"
   },
   "eventMapping": {
     "message": "message",
     "logfile": "log.path",
     "serverHost": "host.hostname",
     "parser": "fields.parser",
     "version": "app.version"
   },
 "delimiter":"\\."
 }
 ```

 `matcher` defines an attribute to determine whether the event mapping applies to the message.
 The event mapping is only applied to messages where the `matcher.attribute` value matches the `matcher.value`.
 `matcher.value` can be a regex.

 `matcher` can also be defined to match all events:
 ```
 "matcher": {
   "matchAll": true
 }
 ```
 When `matchAll=true`, `attribute` and `value` fields are not required and are ignored.

 `eventMapping` defines the message fields that are mapped to Scalyr event attributes.
 The attribute is the Scalyr event key.  The attribute value specifies the
 delimiter separated nested field names for the attribute value.

 The following fields are reserved and have special meaning in Scalyr:
 * `message` - The log message field.  The parser is applied to this field.
 * `logfile` - The logfile name for the log message.
 * `parser` - The Scalyr parser to use to parse the `message` field.
 * `serverHost` - The hostname generating this log message.

 Additional fields can also be specified in the custom app mapping.

 `delimiter` is an optional field an is only needed if the delimiter is not a period.
 */
public class CustomAppEventMapping {
  private Matcher matcher;
  private Map<String, String> eventMapping;
  private String delimiter = "\\.";

  public static final String SERVER_HOST = "serverHost";
  public static final String LOG_FILE = "logfile";
  public static final String PARSER = "parser";
  public static final String MESSAGE = "message";
  private static final String MATCHER_NOT_DEFINED_ERROR = "matcher not defined in custom application event mapping";

  private static final Set<String> standardAttrs = ImmutableSet.of(SERVER_HOST, LOG_FILE, PARSER, MESSAGE);

  public CustomAppEventMapping setMatcher(Matcher matcher) {
    this.matcher = matcher;
    return this;
  }

  public CustomAppEventMapping setEventMapping(Map<String, String> eventMapping) {
    this.eventMapping = eventMapping;
    return this;
  }

  public CustomAppEventMapping setDelimiter(String fieldDelimiter) {
    this.delimiter = fieldDelimiter;
    return this;
  }

  public String getMatcherValue() {
    Preconditions.checkArgument(matcher != null, MATCHER_NOT_DEFINED_ERROR);
    return matcher.value;
  }

  /**
   * All Fields getter methods return List<String> of parsed nested fields
   * using the delimiter as the field separator.
   */
  public List<String> getMatcherFields() {
    Preconditions.checkArgument(matcher != null, MATCHER_NOT_DEFINED_ERROR);
    return splitAttrFields(matcher.attribute);
  }

  public boolean isMatchAll() {
    Preconditions.checkArgument(matcher != null, MATCHER_NOT_DEFINED_ERROR);
    return matcher.matchAll;
  }

  public List<String> getServerHostFields() {
    return getAttribute(SERVER_HOST);
  }

  public List<String> getLogfileFields() {
    return getAttribute(LOG_FILE);
  }

  public List<String> getParserFields() {
    return getAttribute(PARSER);
  }

  public List<String> getMessageFields() {
    return getAttribute(MESSAGE);
  }

  /**
   * Additional message attributes to Map beyond the Scalyr pre-defined fields.
   * @return Map<String, List<String>> where the key is the event attribute
   * and the value is the parsed nested fields for the attribute value.
   */
  public Map<String, List<String>> getAdditionalAttrFields() {
    return eventMapping.entrySet().stream()
      .filter(entry -> !standardAttrs.contains(entry.getKey()))
      .collect(Collectors.toMap(Map.Entry::getKey, entry -> splitAttrFields(entry.getValue())));
  }

  private List<String> getAttribute(String attr) {
    Preconditions.checkArgument(eventMapping != null, "eventMapping not defined in custom application event mapping");
    return splitAttrFields(eventMapping.get(attr));
  }

  private List<String> splitAttrFields(String attrFields) {
    return attrFields == null ? Collections.emptyList() : Arrays.asList(attrFields.split(delimiter));
  }

  /**
   * Defines the field and value to determine whether the message matches the custom app event mapping.
   */
  public static class Matcher {
    private boolean matchAll;
    private String attribute;
    private String value;

    public Matcher setMatchAll(Boolean matchAll) {
      this.matchAll = matchAll;
      return this;
    }

    public Matcher setAttribute(String attribute) {
      this.attribute = attribute;
      return this;
    }

    public Matcher setValue(String value) {
      this.value = value;
      return this;
    }

    @Override
    public String toString() {
      return "Matcher{" +
        "attribute='" + attribute + '\'' +
        ", value='" + value + '\'' +
        '}';
    }
  }

  /**
   * Parses custom app mapping config JSON and constructs `CustomAppEventMapping` from the JSON.
   * @param customAppConfig custom app mapping config JSON in the format: [{customAppEventMapping1}, {customAppEventMapping2}]
   * @return List<CustomAppEventMapping> from the parsed JSON config
   * @throws IOException if custom app event mapping JSON cannot be parsed
   */
  public static List<CustomAppEventMapping> parseCustomAppEventMappingConfig(String customAppConfig) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readValue(customAppConfig, objectMapper.getTypeFactory().constructCollectionType(List.class, CustomAppEventMapping.class));
  }

  @Override
  public String toString() {
    return "CustomAppEventMapping{" +
      "matcher=" + matcher +
      ", eventMapping=" + eventMapping +
      ", delimiter='" + delimiter + '\'' +
      '}';
  }
}
