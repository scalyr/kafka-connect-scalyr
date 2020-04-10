package com.scalyr.integrations.kafka.mapping;

import com.google.common.collect.ImmutableList;
import com.scalyr.api.internal.ScalyrUtil;
import com.scalyr.integrations.kafka.Event;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.Optional;

/**
 * Converts a SinkRecord to a Scalyr Event.
 * Determines which {@link MessageMapper} to use and maps the SinkRecord to an Event using the
 * MessageMapper.
 */
public class ScalyrEventMapper {

  private static final List<MessageMapper> messageMappers = ImmutableList.of(new FilebeatMessageMapper());

  /**
   * Convert a single SinkRecord to a Scalyr Event using a {@link MessageMapper}
   * to map the Event values from the SinkRecord value.
   */
  public Event createEvent(SinkRecord record) {
    MessageMapper messageMapper = getMessageMapper(record);

    return new Event()
      .setTopic(record.topic())
      .setPartition(record.kafkaPartition())
      .setOffset(record.kafkaOffset())
      .setTimestamp(record.timestamp() != null ? record.timestamp() * ScalyrUtil.NANOS_PER_MS : ScalyrUtil.nanoTime())
      .setServerHost(messageMapper.getServerHost(record))
      .setLogfile(messageMapper.getLogfile(record))
      .setParser(messageMapper.getParser(record))
      .setMessage(messageMapper.getMessage(record));
  }


  /**
   * @return MessageMapper for the SinkRecord value
   */
  private MessageMapper getMessageMapper(SinkRecord sinkRecord) {
    Optional<MessageMapper> messageMapper = messageMappers.stream()
      .filter(mapper -> mapper.matches(sinkRecord))
      .findFirst();

    messageMapper.orElseThrow(() -> new DataException("Unsupported message type"));

    return messageMapper.get();
  }
}
