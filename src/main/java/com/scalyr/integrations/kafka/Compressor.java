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
