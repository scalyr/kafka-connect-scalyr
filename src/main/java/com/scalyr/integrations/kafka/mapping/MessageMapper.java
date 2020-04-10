package com.scalyr.integrations.kafka.mapping;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Abstraction for getting event fields from SinkRecord values,
 * which are in different message source formats and can be nested.
 */
public interface MessageMapper {
  String getServerHost(SinkRecord record);
  String getLogfile(SinkRecord record);
  String getParser(SinkRecord record);
  String getMessage(SinkRecord record);

  /**
   * Check message attribute to see if this MessageMapper applies to this message.
   * @return true if the message mapper matches the message.
   */
  boolean matches(SinkRecord record);
}
