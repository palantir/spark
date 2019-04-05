package org.apache.spark.api.shuffle;

import scala.Tuple2;

import java.io.InputStream;
import java.util.Iterator;

public interface ShuffleReaderIterable extends Iterable<ShuffleReaderInputStream> {

  interface ShuffleReaderIterator extends Iterator<ShuffleReaderInputStream> {
    default void retryLastBlock(Throwable t) {
      throw new UnsupportedOperationException("Cannot retry fetching bad blocks", t);
    }
  }

  @Override
  ShuffleReaderIterator iterator();
}
