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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.RateLimiter;
import com.scalyr.api.internal.ScalyrUtil;
import com.scalyr.integrations.kafka.Event;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Converts a SinkRecord to a Scalyr Event.
 * Determines which {@link MessageMapper} to use and maps the SinkRecord to an Event using the
 * MessageMapper.
 */
public class EventMapper {
  private static final Logger log = LoggerFactory.getLogger(EventMapper.class);
  private static final List<MessageMapper> DEFAULT_MAPPERS = ImmutableList.of(new FilebeatMessageMapper());
  private static final List<MessageMapper> messageMappers = new ArrayList<>();
  private final Map<String, String> enrichmentAttrs;
  private final boolean sendEntireRecord;
  private static final RateLimiter noEventMapperLogRateLimiter = RateLimiter.create(1.0/30);  // 1 permit every 30 seconds to not log
  @VisibleForTesting static final String DEFAULT_PARSER = "kafkaParser";

  /**
   * @param enrichmentAttrs Map<String, String> of enrichment key/value pairs
   */
  public EventMapper(Map<String, String> enrichmentAttrs, List<CustomAppEventMapping> customAppEventMappings, boolean sendEntireRecord) {
    this.enrichmentAttrs = enrichmentAttrs;
    this.sendEntireRecord = sendEntireRecord;
    if (customAppEventMappings != null) {
      log.info("Adding custom event mappers {}", customAppEventMappings);
      customAppEventMappings.forEach(customAppEventMapping -> messageMappers.add(new CustomAppMessageMapper(customAppEventMapping)));
    }
    messageMappers.addAll(DEFAULT_MAPPERS);
  }

  /**
   * Convert a single SinkRecord to a Scalyr Event using a {@link MessageMapper}
   * to map the Event values from the SinkRecord value.
   */
  public Event createEvent(SinkRecord record) {
    MessageMapper messageMapper = getMessageMapper(record).orElse(null);
    if (messageMapper == null) {
      if (noEventMapperLogRateLimiter.tryAcquire()) {
        log.warn("No event mapper matches sink record value {}", record.value());
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

    // Supply default parser if parser is null
    Supplier<String> parser = () -> {
      String parserName = messageMapper.getParser(record);
      if (parserName != null) {
        return parserName;
      }
      return DEFAULT_PARSER;
    };

    return new Event()
      .setTopic(record.topic())
      .setPartition(record.kafkaPartition())
      .setOffset(record.kafkaOffset())
      .setTimestamp(record.timestamp() != null ? record.timestamp() * ScalyrUtil.NANOS_PER_MS : ScalyrUtil.nanoTime())
      .setServerHost(serverHost.get())
      .setLogfile(messageMapper.getLogfile(record))
      .setParser(parser.get())
      .setMessage(messageMapper.getMessage(record))
      .setEnrichmentAttrs(enrichmentAttrs)
      .setAdditionalAttrs(messageMapper.getAdditionalAttrs(record));
  }

  /**
   * @return MessageMapper for the SinkRecord value
   */
  private Optional<MessageMapper> getMessageMapper(SinkRecord sinkRecord) {
    final Optional<MessageMapper> messageMapper = messageMappers.stream()
      .filter(mapper -> mapper.matches(sinkRecord))
      .findFirst();

    if (!messageMapper.isPresent() || !sendEntireRecord) {
      return messageMapper;
    }

    return Optional.of(new JsonRecordToMessageMapping(messageMapper.get()));
  }
}
