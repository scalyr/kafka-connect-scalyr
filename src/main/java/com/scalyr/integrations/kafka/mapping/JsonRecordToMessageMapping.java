package com.scalyr.integrations.kafka.mapping;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.ConverterConfig;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Decorator for base MessageMapper which will map the entire Record value to the message field.
 * This is used to send the entire Kafka record value serialized as JSON to Scalyr in the Scalyr event `message` field.
 * The typical use case for this is when the Kafka record value is JSON and the entire JSON needs to be sent to Scalyr.
 */
public class JsonRecordToMessageMapping implements MessageMapper {
  private final MessageMapper baseMapper;
  private final JsonConverter converter;

  public JsonRecordToMessageMapping(MessageMapper baseMapper) {
    this.baseMapper = baseMapper;
    converter = new JsonConverter();
    converter.configure(ImmutableMap.of(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false, ConverterConfig.TYPE_CONFIG, "value"));
  }

  @Override
  public String getServerHost(SinkRecord record) {
    return baseMapper.getServerHost(record);
  }

  @Override
  public String getLogfile(SinkRecord record) {
    return baseMapper.getLogfile(record);
  }

  @Override
  public String getParser(SinkRecord record) {
    return baseMapper.getParser(record);
  }

  @Override
  public String getMessage(SinkRecord record) {
    return new String(converter.fromConnectData(
      record.topic(),
      record.valueSchema(),
      record.value()),
      StandardCharsets.UTF_8);
  }

  @Override
  public Map<String, Object> getAdditionalAttrs(SinkRecord record) {
    return baseMapper.getAdditionalAttrs(record);
  }

  @Override
  public boolean matches(SinkRecord record) {
    return baseMapper.matches(record);
  }
}
