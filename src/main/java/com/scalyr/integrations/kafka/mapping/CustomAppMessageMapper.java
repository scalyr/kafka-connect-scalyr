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

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Maps custom app messages to Scalyr events using {@link CustomAppEventMapping} event mapping definition.
 */
public class CustomAppMessageMapper implements MessageMapper {
  private final List<String> messageFields;
  private final List<String> logfileFields;
  private final List<String> serverHostFields;
  private final List<String> parserFields;
  private final List<String> matcherFields;
  private final Map<String, List<String>> additionalAttrsFields;
  private final Pattern matcherRegex;

  /**
   * CustomApplicationDefinition fields are memoized to instance variables
   * to avoid re-parsing nested fields.
   */
  public CustomAppMessageMapper(CustomAppEventMapping customAppEventMapping) {
    messageFields = customAppEventMapping.getMessageFields();
    logfileFields = customAppEventMapping.getLogfileFields();
    serverHostFields = customAppEventMapping.getServerHostFields();
    parserFields = customAppEventMapping.getParserFields();
    matcherFields = customAppEventMapping.getMatcherFields();
    additionalAttrsFields = customAppEventMapping.getAdditionalAttrFields();
    matcherRegex = Pattern.compile(customAppEventMapping.getMatcherValue());
  }

  @Override
  public String getServerHost(SinkRecord record) {
    return (String)FieldExtractor.getField(record.value(), serverHostFields);
  }

  @Override
  public String getLogfile(SinkRecord record) {
    return (String)FieldExtractor.getField(record.value(), logfileFields);
  }

  @Override
  public String getParser(SinkRecord record) {
    return (String)FieldExtractor.getField(record.value(), parserFields);
  }

  @Override
  public String getMessage(SinkRecord record) {
    return (String)FieldExtractor.getField(record.value(), messageFields);
  }

  @Override
  public Map<String, Object> getAdditionalAttrs(SinkRecord record) {
    Map<String, Object> additionalAttrs = new HashMap<>();
    additionalAttrsFields.forEach((key, value) -> additionalAttrs.put(key, FieldExtractor.getField(record.value(), value)));

    return additionalAttrs;
  }

  @Override
  public boolean matches(SinkRecord record) {
    Object fieldValue = FieldExtractor.getField(record.value(), matcherFields);
    return fieldValue == null ? false : matcherRegex.matcher(fieldValue.toString()).matches();
  }
}
