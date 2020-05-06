package com.scalyr.integrations.kafka;

import com.google.common.collect.ImmutableList;
import com.scalyr.api.internal.ScalyrUtil;
import com.scalyr.integrations.kafka.mapping.FilebeatMessageMapperTest;
import com.scalyr.integrations.kafka.mapping.SinkRecordValueCreator;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Common Utility methods for tests
 */
public class TestUtils {

  /**
   * Create a Map<String, String> of of String[] key/value pairs
   * @param keyValuePairs key1, value1, key2, value2, ...
   * @return Map<String, String> containing specified key value pairs.
   */
  public static Map<String, String> makeMap(String... keyValuePairs) {
    assertEquals("keyValuePairs must be even", 0, keyValuePairs.length % 2);

    Map<String, String> configMap = new HashMap<>();
    for (int i = 0; i < keyValuePairs.length; i+=2) {
      configMap.put(keyValuePairs[i], keyValuePairs[i+1]);
    }
    return configMap;
  }

  /**
   * Verify two maps contain the same values.
   */
  public static void verifyMap(Map<String, String> expected, Map<String, String> actual) {
    assertEquals(expected.size(), actual.size());
    assertEquals(expected.keySet(), actual.keySet());
    expected.keySet().forEach(key -> assertEquals(expected.get(key), actual.get(key)));
  }

  /**
   * TriFunction analogue of Function, BiFunction
   */
  public interface TriFunction<A1, A2, A3, R> {
    R apply(A1 a1, A2 a2, A3 a3);
  }

  /**
   * RecordValueCreators to test
   */
  private static final List<SinkRecordValueCreator> sinkRecordValueCreators = ImmutableList.of(new FilebeatMessageMapperTest.FilebeatSinkRecordValueCreator());

  /**
   * Create test parameters for each SinkRecordValueCreator type
   * that creates a SinkRecord value with the same values for server, logfile, parser.
   * Supplier<Object> that supplies SinkRecord value is returned for the test param.
   */
  public static Collection<Object[]> singleRecordValueTestParams() {
    return sinkRecordValueCreators.stream()
      .flatMap(TestUtils::getSingleRecordValueTestParam)
      .map(param -> new Object[] {param})
      .collect(Collectors.toList());
  }

  /**
   * Create record value suppliers for schemaless (Map) and schema based (Struct) record values.
   * The record value is always the same for server, logfile, and parser.
   * @return Supplier of record value that always has the same values.
   */
  private static Stream<Supplier<Object>> getSingleRecordValueTestParam(SinkRecordValueCreator sinkRecordValueCreator) {
    Supplier<Object> schemalessRecordValueSupplier = () -> sinkRecordValueCreator.createSchemalessRecordValue(1, 1, 1);
    Supplier<Object> schemaRecordValueSupplier = () -> sinkRecordValueCreator.createSchemaRecordValue(1, 1, 1);
    return Stream.of(schemalessRecordValueSupplier, schemaRecordValueSupplier);
  }

  /**
   * Create test parameters for each SinkRecordValueCreator type (e.g. Filebeat)
   * that creates a SinkRecord value with the number of servers, logfiles, and parsers specified in the Trifunction.
   * Trifunction<int numServers, int numLogFiles, int numParsers, Object recordValue> is returned for the test param.
   */
  public static Collection<Object[]> multipleRecordValuesTestParams() {
    return sinkRecordValueCreators.stream()
      .flatMap(TestUtils::getMultipleRecordValuesTestParam)
      .map(param -> new Object[] {param})
      .collect(Collectors.toList());
  }

  /**
   * Create record value function that returns schemaless (Map) and schema based (Struct) record values
   * based on numServers, numLogFiles, numParsers function arguments.
   * @return Trifunction<int numServers, int numLogFiles, int numParsers, Object recordValue>.
   */
  private static Stream<TriFunction<Integer, Integer, Integer, Object>> getMultipleRecordValuesTestParam(SinkRecordValueCreator sinkRecordValueCreator) {
    TriFunction<Integer, Integer, Integer, Object> schemalessRecordValueFn =
      sinkRecordValueCreator::createSchemalessRecordValue;
    TriFunction<Integer, Integer, Integer, Object> schemaRecordValueFn =
      sinkRecordValueCreator::createSchemaRecordValue;

    return Stream.of(schemalessRecordValueFn, schemaRecordValueFn);
  }

  public static List<SinkRecord> createRecords(String topic, int partition, int numRecords, Object recordValue) {
    AtomicInteger offset = new AtomicInteger();
    return IntStream.range(0, numRecords)
      .boxed()
      .map(i -> new SinkRecord(topic, partition, null, null, null, recordValue, offset.getAndIncrement(), ScalyrUtil.currentTimeMillis(), TimestampType.CREATE_TIME))
      .collect(Collectors.toList());
  }

  /**
   * Fails test if Runnable does not throw an Exception or throws an Exception other than the expected exception
   * @param r Runnable
   * @param expectedType Expected Exception.class type
   */
  public static void fails(Runnable r, Class<? extends Throwable> expectedType) {
    fails(() -> { r.run(); return null; }, expectedType::isInstance);
  }

  /**
   * Fails test if Callable does not throw an Exception or Predicate test for the Throwable fails.
   * @param c Callable
   * @param test Predicate to verify Throwable
   */
  public static void fails(Callable<?> c, Predicate<Throwable> test) {
    boolean succeeded = false;
    try {
      c.call();
      succeeded = true;
    } catch (Throwable t) {
      if (test != null && !test.test(t)) {
        throw new RuntimeException("call threw exception (good!), but exception failed check (bad!); (unexpected) exception is: " + t);
      }
    }
    if (succeeded) fail("call should have thrown exception, but did not!");
  }

  /**
   * AddEventsClient performs retries on failure.  Enqueue {@link TestValues#EXPECTED_NUM_RETRIES} MockResponses to the server.
   */
  public static void addMockResponseWithRetries(MockWebServer server, MockResponse mockResponse) {
    IntStream.range(0, TestValues.EXPECTED_NUM_RETRIES).forEach(i -> server.enqueue(mockResponse));
  }

  /**
   * Mock sleep implementation.
   * Tracks total time slept so sleep time and be verified
   * and advances the ScalyrUtil mockable timer to simulate time advancing.
   */
  public static class MockSleep {

    /**
     * Total time slept
     */
    public final AtomicLong sleepTime = new AtomicLong();

    /**
     * Sleep lambda should be called in place of actual sleep
     */
    public final Consumer<Long> sleep = (timeMs) -> {
      sleepTime.addAndGet(timeMs);
      ScalyrUtil.advanceCustomTimeMs(timeMs);
    };

    public MockSleep() {
      ScalyrUtil.setCustomTimeNs(0);
    }

    /**
     * Resets the total sleep time and mockable clock.
     * Should be called each time a new sleep duration needs to be measured.
     */
    public void reset() {
      sleepTime.set(0);
      ScalyrUtil.setCustomTimeNs(0);
    }
  }
}
