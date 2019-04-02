package org.apache.spark.api.shuffle;

public class ShuffleBlockInputStreamId {
  private final int shuffleId;
  private final int mapId;
  private final int reduceId;

  public ShuffleBlockInputStreamId(int shuffleId, int mapId, int reduceId) {
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.reduceId = reduceId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getMapId() {
    return mapId;
  }

  public int getReduceId() {
    return reduceId;
  }
}
