package com.scalyr.integrations.kafka.mapping;

import org.apache.kafka.connect.data.Struct;

import java.util.Map;

/**
 * Test abstraction for creating SinkRecord values from different message sources for testing.
 * It creates the SinkRecord value in the same format as the source to simulate SinkRecords from the source.
 */
public interface SinkRecordValueCreator {
  Map<String, Object> createSchemalessRecordValue(int numServers, int numLogFiles, int numParsers);
  Struct createSchemaRecordValue(int numServers, int numLogFiles, int numParsers);
}
