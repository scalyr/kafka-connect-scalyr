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

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Implementation of {@link Compressor} for different compression algorithms.
 * Provides factory for getting the compression implementation.
 */
public class CompressorFactory {
  public static final String DEFLATE = "deflate";
  public static final String ZSTD = "zstd";
  public static final String NONE = "none";

  public static final List<String> SUPPORTED_COMPRESSION_NAMES = ImmutableList.of(DEFLATE, ZSTD, NONE);

  private CompressorFactory() {
    throw new IllegalStateException("CompressorFactory should be accessed through static methods.");
  }

  /**
   * Simple factory method for {@link Compressor} implementation for the specified compressionType and compressionLevel.
   * @return Compressor for the specified compression type and compression level.
   */
  public static Compressor getCompressor(String commpressionType, Integer compressionLevel) {
    if (DEFLATE.equalsIgnoreCase(commpressionType)) {
      return new DeflateCompressor(compressionLevel);
    } else if (ZSTD.equalsIgnoreCase(commpressionType)) {
      return new ZstdCompressor(compressionLevel);
    } else if (NONE.equalsIgnoreCase(commpressionType)) {
      return new NoCompression();
    }
    throw new IllegalArgumentException("Unsupported compression type " + commpressionType);
  }

  /**
   * Deflate compression implementation
   */
  private static class DeflateCompressor implements Compressor {
    private final int compressionLevel;

    private static final int MIN_COMPRESSION_LEVEL = -1;
    private static final int MAX_COMPRESSION_LEVEL = 9;
    private static final int DEFAULT_COMPRESSION_LEVEL = 6;

    public DeflateCompressor(@Nullable Integer nullableCompressionLevel) {
      int compressionLevel = nullableCompressionLevel == null ? DEFAULT_COMPRESSION_LEVEL : nullableCompressionLevel;
      Preconditions.checkArgument(compressionLevel >= MIN_COMPRESSION_LEVEL && compressionLevel <= MAX_COMPRESSION_LEVEL, "Invalid compression level");
      this.compressionLevel = compressionLevel;
    }

    @Override
    public OutputStream newStreamCompressor(OutputStream baseOutputStream) {
      return new DeflaterOutputStream(baseOutputStream, new Deflater(compressionLevel));
    }

    @Override
    public InputStream newStreamDecompressor(InputStream baseInputStream) {
      return new InflaterInputStream(baseInputStream);
    }

    @Override
    public String getContentEncoding() {
      return "deflate";
    }
  }

  /**
   * Zstandard compression
   */
  private static class ZstdCompressor implements Compressor {
    private final int compressionLevel;

    private static final int MIN_COMPRESSION_LEVEL = 0;
    private static final int MAX_COMPRESSION_LEVEL = 22;
    private static final int DEFAULT_COMPRESSION_LEVEL = 0; // 0 means default, which is controlled by ZSTD_CLEVEL_DEFAULT

    public ZstdCompressor(@Nullable Integer nullableCompressionLevel) {
      int compressionLevel = nullableCompressionLevel == null ? DEFAULT_COMPRESSION_LEVEL : nullableCompressionLevel;
      Preconditions.checkArgument(compressionLevel >= MIN_COMPRESSION_LEVEL && compressionLevel <= MAX_COMPRESSION_LEVEL, "Invalid compression level");
      this.compressionLevel = compressionLevel;
    }

    @Override
    public OutputStream newStreamCompressor(OutputStream baseOutputStream) {
      try {
        return new ZstdOutputStream(baseOutputStream, compressionLevel);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public InputStream newStreamDecompressor(InputStream baseInputStream) {
      try {
        return new ZstdInputStream(baseInputStream);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String getContentEncoding() {
      return "zstd";
    }
  }

  /**
   * No compression implementation
   */
  private static class NoCompression implements Compressor {
    @Override
    public OutputStream newStreamCompressor(OutputStream baseOutputStream) {
      return baseOutputStream;
    }

    @Override
    public InputStream newStreamDecompressor(InputStream baseInputStream) {
      return baseInputStream;
    }

    @Override
    public String getContentEncoding() {
      return "identity";
    }
  }
}
