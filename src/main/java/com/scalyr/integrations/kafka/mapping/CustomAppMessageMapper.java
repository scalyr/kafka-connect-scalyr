package com.scalyr.integrations.kafka.mapping;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  private final String matcherValue;

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
    matcherValue = customAppEventMapping.getMatcherValue();
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
    return matcherValue.equals(fieldValue);
  }
}
