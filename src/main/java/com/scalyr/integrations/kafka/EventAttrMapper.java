package com.scalyr.integrations.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Converts a SinkRecord to event attr Map using a JSON definition of how to convert a nested SinkRecord.value() into the event attr Map.
 * The format of the JSON definition is {eventAttr: [array of hierarchical keys to value]}
 * e.g. {"message" : ["message"],"logfile": ["log", "file", "path"], "serverHost":["host", "hostname"], "parser":["fields", "parser"]}
 */
public class EventAttrMapper {

  public static final String FILEBEATS_EVENT_MAPPING = "{\"message\" : [\"message\"],\"logfile\": [\"log\", \"file\", \"path\"],"
    + " \"source\":[\"host\", \"hostname\"], \"parser\":[\"fields\", \"parser\"]};";

  // TODO: Currently only Filebeats event mapping is supported.  In the future, additional event mappings will be supported.
  private Map<String, List<String>> eventMapping;

  public EventAttrMapper() {
    // Parse event mapping JSON
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      Map parsedJson = objectMapper.readValue(FILEBEATS_EVENT_MAPPING, Map.class);
      parsedJson.values().forEach(mappingKeys -> Preconditions.checkArgument(mappingKeys instanceof List));
      eventMapping = (Map<String, List<String>>) parsedJson;
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create Event attrs from SinkRecord using event mapping
   * @param record
   * @return Event attrs
   */
  public Map<String, Object> convert(SinkRecord record) {
    Map recordValue = (Map)record.value();

    Map<String, Object> eventAttr = new HashMap<>();
    eventMapping.forEach((attr, attrKeys) -> {
      Optional attrValue = getField(recordValue, attrKeys);
      attrValue.ifPresent(val -> eventAttr.put(attr, val));
    });

    return eventAttr;
  }

  /**
   * @return field value specified by the hierarchical keys from recordValue.  Empty Optional if the field does not exist.
   */
  private Optional<Object> getField(Map<String, Object> recordValue, List<String> keys) {
    Object fieldValue = null;
    for (String key : keys) {
      fieldValue = recordValue.get(key);

      // field doesn't exist
      if (fieldValue == null) {
        return Optional.empty();
      }

      if (fieldValue instanceof Map) {
        recordValue = (Map) fieldValue;
      }
    }
    return Optional.ofNullable(fieldValue);
  }
}
