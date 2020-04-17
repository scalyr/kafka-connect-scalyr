package com.scalyr.integrations.kafka.mapping;

import com.scalyr.integrations.kafka.TestUtils;
import com.scalyr.integrations.kafka.TestValues;
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
   * Test mapper gets correct values
   */
  @Test
  public void testFileBeatsMessageMapper() {
    SinkRecord record = new SinkRecord(topic, partition, null, null, null, sinkRecordValueCreator.createSchemaless(1, 1, 1), offset.getAndIncrement());
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
    public Map<String, Object> createSchemaless(int numServers, int numLogFiles, int numParsers) {
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
  }
}
