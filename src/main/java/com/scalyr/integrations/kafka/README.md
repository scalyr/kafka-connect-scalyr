Scalyr Kafka Connect Data Flow
--------------------------------------------------------------------------------

```
                      createEvent +-------------+  getField  +---------------+  getField  +----------------+
                     +----------->+ EventMapper +----------->+ MessageMapper +----------->+ FieldExtractor |
+----------------+   |            +-------------+            +---------------+            +----------------+
| ScalyrSinkTask +---+
+----------------+   |
                     | POST Event  +-----------------+
                     +------------>+ AddEventsClient |
                                   +-----------------+
```

1. `ScalyrSinkTask` calls `EventMapper.createEvent` to create an `Event` from a `SinkRecord`.
2. `EventMapper` determines the `MessageMapper` implementation to use to based on the `SinkRecord` value fields.
The `MessageMapper` implementation has the logic for mapping nested `SinkRecord` value fields to `Event` fields.
3. `EventMapper` gets the `Event` fields from the `SinkRecord` value using the `MessageMapper`.
4. `MessageMapper` calls `FieldExtractor.getField` to get the nested fields from the `SinkRecord` value.
5. `ScalyrSinkTask` calls `AddEventsClient` to send the `Event` to the Scalyr addEvents API. 
