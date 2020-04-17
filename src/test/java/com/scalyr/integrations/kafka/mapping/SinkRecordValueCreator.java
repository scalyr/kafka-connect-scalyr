package com.scalyr.integrations.kafka.mapping;

import java.util.Map;

/**
 * Test abstraction for creating SinkRecord values from different message sources for testing.
 * It creates the SinkRecord value in the same format as the source to simulate SinkRecords from the source.
 */
public interface SinkRecordValueCreator {
  Map<String, Object> createSchemaless(int numServers, int numLogFiles, int numParsers);
  // TODO: Add method for creating schema-based records
}
