package com.scalyr.integrations.kafka.mapping;

import com.scalyr.api.internal.ScalyrUtil;
import com.scalyr.integrations.kafka.Event;
import com.scalyr.integrations.kafka.TestUtils;
import com.scalyr.integrations.kafka.TestValues;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Test for EventMapper
 */
@RunWith(Parameterized.class)
public class EventMapperTest {

  private static final String topic = "test-topic";
  private static final int partition = 0;
  private static final List<List<String>> testEnrichmentAttrs =
    Arrays.asList(null, Arrays.asList(TestValues.ENRICHMENT_VALUE.split(","))); // Test without and with enrichment
  private static final AtomicInteger offset = new AtomicInteger();

  private final Supplier<Object> recordValue;
  private final List<String> enrichmentAttrs;
  private EventMapper eventMapper;

  /**
   * Create test parameters for each SinkRecordValueCreator type and testEnrichmentAttrs combination.
   * Object[] = {Supplier<Object> recordValue, List<String> enrichmentAttr}
   */
  @Parameterized.Parameters
  public static Collection<Object[]> testParams() {
    return TestUtils.singleRecordValueTestParams().stream()
      .flatMap(recordValue -> testEnrichmentAttrs.stream().map(enrichmentAttrs -> new Object[] {recordValue[0], enrichmentAttrs}))
      .collect(Collectors.toList());
  }

  public EventMapperTest(Supplier<Object> recordValue, List<String> enrichmentAttrs) {
    this.recordValue = recordValue;
    this.enrichmentAttrs = enrichmentAttrs;
  }

  @Before
  public void setup() {
    this.eventMapper = new EventMapper(enrichmentAttrs);
  }

  /**
   * Test EventMapper with no timestamp in SinkRecord
   */
  @Test
  public void createEventNoTimestampTest() {
    // Without timestamp
    final long nsFromEpoch = ScalyrUtil.NANOS_PER_SECOND;  // 1 second after epoch
    ScalyrUtil.setCustomTimeNs(nsFromEpoch);
    SinkRecord sinkRecord = new SinkRecord(topic, partition, null, null, null, recordValue.get(), offset.getAndIncrement());
    Event event = eventMapper.createEvent(sinkRecord);
    validateEvent(event);
    assertEquals(nsFromEpoch, event.getTimestamp());
  }

  /**
   * Test EventMapper with timestamp in SinkRecord
   */
  @Test
  public void crateEventWithTimestampTest() {
    // With timestamp
    final long msSinceEpoch = 60 * 1000;  // 1 minute after epoch
    SinkRecord sinkRecord = new SinkRecord(topic, partition, null, null, null, recordValue.get(), offset.getAndIncrement(), msSinceEpoch, TimestampType.CREATE_TIME);
    Event event = eventMapper.createEvent(sinkRecord);
    validateEvent(event);
    assertEquals(msSinceEpoch * ScalyrUtil.NANOS_PER_MS, event.getTimestamp());
  }


  /**
   * Test no {@link MessageMapper} for the SinkRecord value
   */
  @Test(expected = DataException.class)
  public void noMessageMapperTest() {
    SinkRecord sinkRecord = new SinkRecord(topic, partition, null, null, null, new HashMap<>(), offset.getAndIncrement());
    eventMapper.createEvent(sinkRecord);
  }

  /**
   * Validate Scalyr event matches SinkRecord
   */
  private void validateEvent(Event event) {
    assertEquals(TestValues.MESSAGE_VALUE, event.getMessage());
    assertEquals(TestValues.LOGFILE_VALUE + "0", event.getLogfile());
    assertEquals(TestValues.PARSER_VALUE + "0", event.getParser());
    assertEquals(TestValues.SERVER_VALUE + "0", event.getServerHost());
    assertEquals(topic, event.getTopic());
    assertEquals(partition, event.getPartition());
    assertEquals(offset.get() - 1, event.getOffset());
    assertEquals(convertEnrichmentAttrs(enrichmentAttrs), event.getAdditionalAttrs());
  }

  /**
   * Convert List<String> of key=value into Map<String, Object>
   */
  private Map<String, Object> convertEnrichmentAttrs(List<String> enrichmentAttrs) {
    if (enrichmentAttrs == null) {
      return null;
    }

    return enrichmentAttrs.stream()
      .map(enrichmentAttr -> enrichmentAttr.split("=", 2))
      .collect(Collectors.toMap(keyValue -> keyValue[0], keyValue -> keyValue[1]));
  }
}
