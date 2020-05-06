package com.scalyr.integrations.kafka;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.io.IOException;
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
  public static final String DEFLATE = "deflate";
  public static final String ZSTD = "zstd";
  public static final String NONE = "none";

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
   * Zstandard compression
   */
  private static class ZstdCompressor implements Compressor {
    private final int compressionLevel;

    private static final int minCompressionLevel = 0;
    private static final int maxCompressionLevel = 22;
    private static final int defaultCompressionLevel = 0; // 0 means default, which is controlled by ZSTD_CLEVEL_DEFAULT

    public ZstdCompressor(@Nullable Integer nullableCompressionLevel) {
      int compressionLevel = nullableCompressionLevel == null ? defaultCompressionLevel : nullableCompressionLevel.intValue();
      Preconditions.checkArgument(compressionLevel >= minCompressionLevel && compressionLevel <= maxCompressionLevel, "Invalid compression level");
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
