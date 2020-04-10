package com.scalyr.integrations.kafka.mapping;

import com.google.common.collect.ImmutableList;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;

/**
 Filebeat MessageMapper implementation to extract fields from Filebeat messages.

 Sample Filebeat message with fields of interest:
```
{
  "message": "xxx",
  "host": {
    "hostname": "Test-Host",
    "name": "Test-Host"
  },
  "log": {
    "file": {
      "path": "/var/log/system.log"
    },
  },
  agent": {
    "type": "filebeat"
  },
  fields": {
    "parser":"systemLog"
  }
}
```
 */
public class FilebeatMessageMapper implements MessageMapper {
  private static final List<String> MESSAGE_FIELDS = ImmutableList.of("message");
  private static final List<String> LOGFILE_FIELDS = ImmutableList.of("log", "file", "path");
  private static final List<String> SERVERHOST_FIELDS = ImmutableList.of("host", "hostname");
  private static final List<String> PARSER_FIELDS = ImmutableList.of("fields", "parser");
  private static final List<String> SOURCE_TYPE_FIELDS = ImmutableList.of("agent", "type");
  private static final String FILEBEAT = "filebeat";

  @Override
  public String getServerHost(SinkRecord record) {
    return (String)FieldExtractor.getField(record.value(), SERVERHOST_FIELDS);
  }

  @Override
  public String getLogfile(SinkRecord record) {
    return (String)FieldExtractor.getField(record.value(), LOGFILE_FIELDS);
  }

  @Override
  public String getParser(SinkRecord record) {
    return (String)FieldExtractor.getField(record.value(), PARSER_FIELDS);
  }

  @Override
  public String getMessage(SinkRecord record) {
    return (String)FieldExtractor.getField(record.value(), MESSAGE_FIELDS);
  }

  @Override
  public boolean matches(SinkRecord record) {
    return FILEBEAT.equals(FieldExtractor.getField(record.value(), SOURCE_TYPE_FIELDS));
  }
}
