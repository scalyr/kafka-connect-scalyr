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

import com.google.common.util.concurrent.RateLimiter;
import com.scalyr.api.internal.ScalyrUtil;
import com.scalyr.integrations.kafka.Event;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Converts a SinkRecord to a Scalyr Event.
 * Determines which {@link MessageMapper} to use and maps the SinkRecord to an Event using the
 * MessageMapper.
 */
public class EventMapper {
  private static final Logger log = LoggerFactory.getLogger(EventMapper.class);
  private static final List<MessageMapper> messageMappers = Stream.of(new FilebeatMessageMapper()).collect(Collectors.toList());
  private final Map<String, String> enrichmentAttrs;
  private static final RateLimiter noEventMapperLogRateLimiter = RateLimiter.create(1.0/30);  // 1 permit every 30 seconds to not log

  /**
   * @param enrichmentAttrs Map<String, String> of enrichment key/value pairs
   */
  public EventMapper(Map<String, String> enrichmentAttrs, List<CustomAppEventMapping> customAppEventMappings) {
    this.enrichmentAttrs = enrichmentAttrs;
    if (customAppEventMappings != null) {
      log.info("Adding custom event mappers {}", customAppEventMappings);
      customAppEventMappings.forEach(customAppEventMapping -> messageMappers.add(new CustomAppMessageMapper(customAppEventMapping)));
    }
  }

  /**
   * Convert a single SinkRecord to a Scalyr Event using a {@link MessageMapper}
   * to map the Event values from the SinkRecord value.
   */
  public Event createEvent(SinkRecord record) {
    MessageMapper messageMapper = getMessageMapper(record).orElse(null);
    if (messageMapper == null) {
      if (noEventMapperLogRateLimiter.tryAcquire()) {
        log.info("No event mapper matches sink record value {}", record.value());
      }
      return null;
    }

    // Supply default for serverHost if null without creating unneeded default Strings
    Supplier<String> serverHost = () -> {
      String hostname = messageMapper.getServerHost(record);
      if (hostname != null) {
        return hostname;
      }
      return "Kafka-" + record.topic();
    };

    return new Event()
      .setTopic(record.topic())
      .setPartition(record.kafkaPartition())
      .setOffset(record.kafkaOffset())
      .setTimestamp(record.timestamp() != null ? record.timestamp() * ScalyrUtil.NANOS_PER_MS : ScalyrUtil.nanoTime())
      .setServerHost(serverHost.get())
      .setLogfile(messageMapper.getLogfile(record))
      .setParser(messageMapper.getParser(record))
      .setMessage(messageMapper.getMessage(record))
      .setEnrichmentAttrs(enrichmentAttrs)
      .setAdditionalAttrs(messageMapper.getAdditionalAttrs(record));
  }

  /**
   * @return MessageMapper for the SinkRecord value
   */
  private Optional<MessageMapper> getMessageMapper(SinkRecord sinkRecord) {
    return messageMappers.stream()
      .filter(mapper -> mapper.matches(sinkRecord))
      .findFirst();
  }
}
