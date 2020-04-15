package com.scalyr.integrations.kafka;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Abstraction for compression with different compression algorithms
 */
interface Compressor {
  OutputStream compressStream(OutputStream out);
  InputStream decompressStream(InputStream in);
  String getContentEncoding();
}
