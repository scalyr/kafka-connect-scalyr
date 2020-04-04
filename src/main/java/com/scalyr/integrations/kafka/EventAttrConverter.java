package com.scalyr.integrations.kafka;

import org.apache.kafka.connect.sink.SinkRecord;
import java.util.Map;

/**
 * Interface for converting SinkRecord to event attr Map.
 */
@FunctionalInterface
public interface EventAttrConverter {
  Map<String, Object> convert(SinkRecord record);
}
