package com.scalyr.integrations.kafka;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Implementation of {@link Compressor} for different compression algorithms.
 * Provides factory for getting the compression implementation.
 */
public class CompressorFactory {
  private static final String DEFLATE = "deflate";
  private static final String NONE = "none";

  /**
   * Simple factory method for {@link Compressor} implementation for the specified compressionType and compressionLevel.
   * @return Compressor for the specified compression type and compression level.
   */
  public static Compressor getCompressor(String commpressionType, Integer compressionLevel) {
    if (DEFLATE.equalsIgnoreCase(commpressionType)) {
      return new DeflateCompressor(compressionLevel);
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

    private static final int minCompressionLevel = -1;
    private static final int maxCompressionLevel = 9;
    private static final int defaultCompressionLevel = 6;

    public DeflateCompressor(@Nullable Integer nullableCompressionLevel) {
      int compressionLevel = nullableCompressionLevel == null ? defaultCompressionLevel : nullableCompressionLevel.intValue();
      Preconditions.checkArgument(compressionLevel >= minCompressionLevel && compressionLevel <= maxCompressionLevel, "Invalid compression level");
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
