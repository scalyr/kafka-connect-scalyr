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

import com.google.common.io.ByteStreams;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import static com.scalyr.integrations.kafka.TestUtils.fails;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * CompressorFactory test
 */
public class CompressorFactoryTest {
  private static byte[] testData;

  private static final String DEFLATE = "deflate";
  private static final String ZSTD = "zstd";
  private static final String NONE = "none";
  private static final int testDataSize = 4000;

  @BeforeClass
  public static void createTestData() {
    testData = new byte[testDataSize];
    Arrays.fill(testData, (byte)'a');
  }

  /**
   * Round trip test of deflate and inflate
   */
  @Test
  public void testDeflate() throws IOException {
    Compressor compressor = CompressorFactory.getCompressor(DEFLATE, null);
    ByteArrayOutputStream compressedOutputStream = new ByteArrayOutputStream();
    try (OutputStream uncompressedOutputStream = compressor.newStreamCompressor(compressedOutputStream)) {
      uncompressedOutputStream.write(testData);
    }

    InputStream decompressedInputStream = compressor.newStreamDecompressor(new ByteArrayInputStream(compressedOutputStream.toByteArray()));
    assertArrayEquals(testData, ByteStreams.toByteArray(decompressedInputStream));
    assertEquals("deflate", compressor.getContentEncoding());
  }

  /**
   * Verify invalid and valid deflate compression levels
   */
  @Test
  public void testDeflateCompressionLevel() {
    fails(() -> CompressorFactory.getCompressor(DEFLATE, 12), IllegalArgumentException.class);
    fails(() -> CompressorFactory.getCompressor(DEFLATE, -2), IllegalArgumentException.class);

    // Boundary conditions - ok
    CompressorFactory.getCompressor(DEFLATE, -1);
    CompressorFactory.getCompressor(DEFLATE, 0);
    CompressorFactory.getCompressor(DEFLATE, 9);
    CompressorFactory.getCompressor(DEFLATE, null);
  }

  /**
   * Round trip test of deflate and inflate
   */
  @Test
  public void testZStandard() throws IOException {
    Compressor compressor = CompressorFactory.getCompressor(ZSTD, null);
    ByteArrayOutputStream compressedOutputStream = new ByteArrayOutputStream();
    try (OutputStream uncompressedOutputStream = compressor.newStreamCompressor(compressedOutputStream)) {
      uncompressedOutputStream.write(testData);
    }

    InputStream decompressedInputStream = compressor.newStreamDecompressor(new ByteArrayInputStream(compressedOutputStream.toByteArray()));
    assertArrayEquals(testData, ByteStreams.toByteArray(decompressedInputStream));
    assertEquals("zstd", compressor.getContentEncoding());
  }

  /**
   * Verify invalid and valid deflate compression levels
   */
  @Test
  public void testZStandardCompressionLevel() {
    fails(() -> CompressorFactory.getCompressor(ZSTD, 23), IllegalArgumentException.class);
    fails(() -> CompressorFactory.getCompressor(ZSTD, -1), IllegalArgumentException.class);

    // Boundary conditions - ok
    CompressorFactory.getCompressor(ZSTD, 0);
    CompressorFactory.getCompressor(ZSTD, 22);
    CompressorFactory.getCompressor(ZSTD, null);
  }

  /**
   * Round trip test for no compression
   */
  @Test
  public void testNoCompression() throws IOException {
    Compressor compressor = CompressorFactory.getCompressor(NONE, 0);
    ByteArrayOutputStream compressedOutputStream = new ByteArrayOutputStream();
    try (OutputStream uncompressedOutputStream = compressor.newStreamCompressor(compressedOutputStream)) {
      uncompressedOutputStream.write(testData);
    }
    assertEquals(testDataSize, compressedOutputStream.size());

    InputStream decompressedInputStream = compressor.newStreamDecompressor(new ByteArrayInputStream(compressedOutputStream.toByteArray()));
    assertArrayEquals(testData, ByteStreams.toByteArray(decompressedInputStream));
    assertEquals("identity", compressor.getContentEncoding());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidCompressionType() {
    CompressorFactory.getCompressor("Invalid", 0);
  }
}
