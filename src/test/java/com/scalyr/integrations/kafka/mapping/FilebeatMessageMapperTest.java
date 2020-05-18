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

import com.scalyr.integrations.kafka.TestUtils;
import com.scalyr.integrations.kafka.TestValues;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for Filebeat MessageMapper
 */
public class FilebeatMessageMapperTest {
  private static final AtomicInteger offset = new AtomicInteger();
  private static final Random random = new Random();
  private MessageMapper messageMapper;
  private SinkRecordValueCreator sinkRecordValueCreator;

  private static final String topic = "test-topic";
  private static final int partition = 0;

  @Before
  public void setup() {
    messageMapper = new FilebeatMessageMapper();
    sinkRecordValueCreator = new FilebeatSinkRecordValueCreator();
  }

  /**
   * Test mapper gets correct values for schemaless record value
   */
  @Test
  public void testFileBeatsMessageMapperSchemaless() {
    SinkRecord record = new SinkRecord(topic, partition, null, null, null, sinkRecordValueCreator.createSchemalessRecordValue(1, 1, 1), offset.getAndIncrement());
    verifySinkRecord(record);
  }

  /**
   * Test mapper gets correct values for schema record value
   */
  @Test
  public void testFileBeatsMessageMapperSchema() {
    SinkRecord record = new SinkRecord(topic, partition, null, null, null, sinkRecordValueCreator.createSchemaRecordValue(1, 1, 1), offset.getAndIncrement());
    verifySinkRecord(record);
  }

  private void verifySinkRecord(SinkRecord record) {
    assertEquals(TestValues.MESSAGE_VALUE, messageMapper.getMessage(record));
    assertEquals(TestValues.LOGFILE_VALUE + "0", messageMapper.getLogfile(record));
    assertEquals(TestValues.PARSER_VALUE + "0", messageMapper.getParser(record));
    assertEquals(TestValues.SERVER_VALUE + "0", messageMapper.getServerHost(record));
    assertTrue(messageMapper.matches(record));
  }

  /**
   * Test mapped fields not present
   */
  @Test
  public void testMissingFieldsMessage() {
    SinkRecord record = new SinkRecord(topic, partition, null, null, null, new HashMap<>(), offset.getAndIncrement());
    assertNull(messageMapper.getMessage(record));
    assertNull(messageMapper.getLogfile(record));
    assertNull(messageMapper.getParser(record));
    assertNull(messageMapper.getServerHost(record));
    assertFalse(messageMapper.matches(record));
  }

  /**
   * Creates SinkRecord values in the Filebeat format.
   */
  public static class FilebeatSinkRecordValueCreator implements SinkRecordValueCreator {
    @Override
    public Map<String, Object> createSchemalessRecordValue(int numServers, int numLogFiles, int numParsers) {
      assertTrue(numParsers <= numLogFiles);

      Map<String, Object> value = new HashMap<>();
      // {message: logMessage}
      value.put("message", TestValues.MESSAGE_VALUE);

      // {host: {hostname: myhost}}
      value.put("host", TestUtils.makeMap("hostname", TestValues.SERVER_VALUE + random.nextInt(numServers)));

      // {log: {file: {path: /var/log/syslog}}};
      final Map<String, Object> file_path = new HashMap<>();
      final int logFileNum = random.nextInt(numLogFiles);
      file_path.put("file", TestUtils.makeMap("path", TestValues.LOGFILE_VALUE + logFileNum));
      value.put("log", file_path);

      // {fields: {parser: systemLog}}
      value.put("fields", TestUtils.makeMap("parser", TestValues.PARSER_VALUE + logFileNum % numParsers));

      // {agent: {type: filebeat}}
      value.put("agent", TestUtils.makeMap("type", "filebeat"));

      return value;
    }

    /**
     * @return Struct Test SinkRecord value with Schema
     */
    @Override
    public Struct createSchemaRecordValue(int numServers, int numLogFiles, int numParsers) {
      assertTrue(numParsers <= numLogFiles);

      final Schema hostSchema = SchemaBuilder.struct().name("host")
        .field("hostname", Schema.STRING_SCHEMA).build();

      final Schema fileSchema = SchemaBuilder.struct().name("file")
        .field("path", Schema.STRING_SCHEMA);

      final Schema logSchema = SchemaBuilder.struct().name("log")
        .field("file", fileSchema);

      final Schema parserSchema = SchemaBuilder.struct().name("parser")
        .field("parser", Schema.STRING_SCHEMA);

      final Schema agentSchema = SchemaBuilder.struct().name("agent")
        .field("type", Schema.STRING_SCHEMA);

      // filebeats doesn't support schemas, but we create a schema for filebeats to test schema support with the same data set
      final Schema filebeatsSchema = SchemaBuilder.struct().name("fileBeats")
        .field("message", Schema.STRING_SCHEMA)
        .field("host", hostSchema)
        .field("log", logSchema)
        .field("fields", parserSchema)
        .field("agent", agentSchema)
        .build();

      Struct value = new Struct(filebeatsSchema);
      value.put("message", TestValues.MESSAGE_VALUE);
      value.put("host", new Struct(hostSchema).put("hostname", TestValues.SERVER_VALUE + random.nextInt(numServers)));

      final int logFileNum = random.nextInt(numLogFiles);
      value.put("log", new Struct(logSchema).put("file", new Struct(fileSchema).put("path", TestValues.LOGFILE_VALUE + logFileNum)));
      value.put("fields", new Struct(parserSchema).put("parser", TestValues.PARSER_VALUE + logFileNum % numParsers));
      value.put("agent", new Struct(agentSchema).put("type", "filebeat"));

      return value;
    }
  }
}
