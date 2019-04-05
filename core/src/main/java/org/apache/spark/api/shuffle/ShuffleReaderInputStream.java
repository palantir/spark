package org.apache.spark.api.shuffle;

import java.io.InputStream;

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
