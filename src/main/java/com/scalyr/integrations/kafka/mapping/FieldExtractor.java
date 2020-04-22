package com.scalyr.integrations.kafka.mapping;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.util.List;
import java.util.Map;

/**
 * Extracts nested field values from the SinkRecord value.
 */
public class FieldExtractor {
  /**
   * Call the correct {@link #getField} based on the recordValue type.
   */
  public static Object getField(Object recordValue, List<String> keys) {
    if (recordValue instanceof Map) {
      // Schemaless SinkRecord value
      return getField((Map)recordValue, keys);
    } else if (recordValue instanceof Struct) {
      // Schema-based SinkRecord value
      return getField((Struct) recordValue, keys);
    }
    throw new DataException("Unsupported data type " + recordValue.getClass().getName());
  }

  /**
   * Extract field value specified by the nested keys.
   * @param recordValue Schemaless SinkRecord value Map
   * @param keys Nested keys to extract the value from.  Highest level key is first in the List.
   * @return Field value specified by the nested keys from recordValue.  Null if the field does not exist.
   * @throws DataException if field is a nested data type
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
    validateFieldValue(fieldValue);
    return fieldValue;
  }

  /**
   * Extract field from SinkRecord value with Schema, which is represented as a Struct.
   * @return field value specified by the hierarchical keys from recordValue.  Null if the field does not exist.
   * @throws DataException if field is a nested data type
   */
  private static Object getField(Struct recordValue, List<String> keys) {
    Object fieldValue = null;

    for (String key : keys) {
      Schema schema = recordValue.schema();

      // field doesn't exist
      if (schema.type() == Schema.Type.STRUCT && schema.field(key) == null) {
        return null;
      }

      fieldValue = recordValue.get(key);

      if (fieldValue instanceof Struct) {
        recordValue = (Struct) fieldValue;
      }
    }
    validateFieldValue(fieldValue);
    return fieldValue;
  }

  /**
   * Verify field value is not a nested data type.
   * @throws DataException if nested data type
   */
  private static void validateFieldValue(Object fieldValue) {
    if (fieldValue instanceof Map || fieldValue instanceof Struct) {
      throw new DataException("Nested data types not supported");
    }
  }
}
