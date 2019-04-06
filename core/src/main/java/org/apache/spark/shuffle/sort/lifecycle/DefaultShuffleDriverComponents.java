package org.apache.spark.shuffle.sort.lifecycle;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.shuffle.ShuffleDataCleaner;
import org.apache.spark.api.shuffle.ShuffleDriverComponents;
import org.apache.spark.internal.config.package$;
import org.apache.spark.storage.BlockManagerMaster;

public class DefaultShuffleDriverComponents implements ShuffleDriverComponents {

  private final boolean blockOnShuffleCleanup;
  private BlockManagerMaster blockManagerMaster;

  public DefaultShuffleDriverComponents(SparkConf sparkConf) {
    this.blockOnShuffleCleanup = (boolean) sparkConf.get(package$.MODULE$.CLEANER_REFERENCE_TRACKING_BLOCKING_SHUFFLE());
  }

  @Override
  public void initializeApplication() {
    blockManagerMaster = SparkEnv.get().blockManager().master();
  }

  @Override
  public void cleanupApplication() {
    // do nothing
  }

  @Override
  public ShuffleDataCleaner dataCleaner() {
    checkInitialized();
    return new DefaultShuffleDataCleaner(blockManagerMaster, blockOnShuffleCleanup);
  }

  private void checkInitialized() {
    if (blockManagerMaster == null) {
      throw new IllegalStateException("Driver components must be initialized before using");
    }
  }
}
