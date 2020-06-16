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

package com.scalyr.integrations.kafka;

/**
 * Defines version of Scalyr Kafka Connect Sink
 */
class VersionUtil {

  private VersionUtil() {
    throw new IllegalStateException("Utility class");
  }

  public static final String VERSION = "0.1.0.0";
  public static String getVersion() {
    return VERSION;
  }
}
