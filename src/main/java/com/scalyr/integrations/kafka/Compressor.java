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

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Abstraction for compression that supports different compression algorithms.
 */
interface Compressor {
  /**
   * Creates a new OutputStream that performs compression.
   * @param baseOutputStream underlying OutputStream to write to
   * @return Compressed OutputStream that wraps `baseOutputStream`
   */
  OutputStream newStreamCompressor(OutputStream baseOutputStream);

  /**
   * Creates a new InputStream that performs decompression.
   * @param baseInputStream underlying InputStream to read from
   * @return Decompressed InputStream that wraps `baseInputStream`
   */
  InputStream newStreamDecompressor(InputStream baseInputStream);

  /**
   * @return Content-Encoding http header String for this compression type
   */
  String getContentEncoding();
}
