package org.apache.spark.api.shuffle;

import org.apache.spark.annotation.Experimental;

import java.util.Iterator;

/**
 * :: Experimental ::
 * An interface for iterating through shuffle blocks to read.
 * @since 3.0.0
 */
@Experimental
public interface ShuffleReaderIterable extends Iterable<ShuffleReaderInputStream> {

  interface ShuffleReaderIterator extends Iterator<ShuffleReaderInputStream> {
    /**
     * Instructs the shuffle iterator to fetch the last block again. This is useful
     * if the block is determined to be corrupt after decryption or decompression.
     */
    default void retryLastBlock(Throwable t) {
      throw new UnsupportedOperationException("Cannot retry fetching bad blocks", t);
    }
  }

  @Override
  ShuffleReaderIterator iterator();
}
