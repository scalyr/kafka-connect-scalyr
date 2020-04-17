package com.scalyr.integrations.kafka.mapping;

import org.apache.kafka.connect.errors.DataException;

import java.util.List;
import java.util.Map;

/**
 * Extracts nested field values from the SinkRecord value.
 */
public class FieldExtractor {
  /**
   * Call the correct {@link #getField} based on the recordValue type.
   * TODO: Add Schema struct support
   */
  public static Object getField(Object recordValue, List<String> keys) {
    // Schemaless SinkRecord value
    if (recordValue instanceof Map) {
      return getField((Map)recordValue, keys);
    }
    throw new DataException("Unsupported data type " + recordValue.getClass().getName());
  }

  /**
   * Extract field value specified by the nested keys.
   * @param recordValue Schemaless SinkRecord value Map
   * @param keys Nested keys to extract the value from.  Highest level key is first in the List.
   * @return Field value specified by the nested keys from recordValue.  Null if the field does not exist.
   */
  public static Object getField(Map<String, Object> recordValue, List<String> keys) {
    Object fieldValue = null;
    for (String key : keys) {
      fieldValue = recordValue.get(key);

      // field doesn't exist
      if (fieldValue == null) {
        return null;
      }

      if (fieldValue instanceof Map) {
        recordValue = (Map) fieldValue;
      }
    }
    return fieldValue;
  }
}
