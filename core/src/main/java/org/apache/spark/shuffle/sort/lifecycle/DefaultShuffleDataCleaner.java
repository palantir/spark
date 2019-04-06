package org.apache.spark.shuffle.sort.lifecycle;

import org.apache.spark.api.shuffle.ShuffleDataCleaner;
import org.apache.spark.storage.BlockManagerMaster;

import java.io.IOException;

public class DefaultShuffleDataCleaner implements ShuffleDataCleaner {

  private final BlockManagerMaster blockManagerMaster;

  /**
   * Whether the cleaning thread will block on shuffle cleanup tasks.
   *
   * When context cleaner is configured to block on every delete request, it can throw timeout
   * exceptions on cleanup of shuffle blocks, as reported in SPARK-3139. To avoid that, this
   * parameter by default disables blocking on shuffle cleanups. Note that this does not affect
   * the cleanup of RDDs and broadcasts. This is intended to be a temporary workaround,
   * until the real RPC issue (referred to in the comment above `blockOnCleanupTasks`) is
   * resolved.
   */
  private final boolean blockOnShuffleCleanup;

  public DefaultShuffleDataCleaner(BlockManagerMaster blockManagerMaster, boolean blockOnShuffleCleanup) {
    this.blockManagerMaster = blockManagerMaster;
    this.blockOnShuffleCleanup = blockOnShuffleCleanup;
  }

  @Override
  public void removeShuffleData(int shuffleId) throws IOException {
    blockManagerMaster.removeShuffle(shuffleId, blockOnShuffleCleanup);
  }
}
