package org.apache.spark.api.shuffle;

import org.apache.spark.annotation.Experimental;

import java.io.InputStream;

/**
 * :: Experimental ::
 * An interface for reading shuffle records.
 * @since 3.0.0
 */
@Experimental
public class ShuffleReaderInputStream {

  private final ShuffleBlockInfo shuffleBlockInfo;
  private final InputStream inputStream;

  public ShuffleReaderInputStream(ShuffleBlockInfo shuffleBlockInfo, InputStream inputStream) {
    this.shuffleBlockInfo = shuffleBlockInfo;
    this.inputStream = inputStream;
  }

  public ShuffleBlockInfo getShuffleBlockInfo() {
    return shuffleBlockInfo;
  }

  public InputStream getInputStream() {
    return inputStream;
  }
}
