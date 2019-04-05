package org.apache.spark.api.shuffle;

import scala.Tuple2;

import java.io.InputStream;
import java.util.Iterator;

public interface ShuffleReaderIterable extends Iterable<Tuple2<ShuffleBlockInfo, InputStream>> {
  interface ShuffleReaderIterator extends Iterator<Tuple2<ShuffleBlockInfo, InputStream>> {
    default void retryLastBlock(Throwable t) {
      throw new UnsupportedOperationException("Cannot retry fetching bad blocks", t);
    }
  }
  @Override
  ShuffleReaderIterator iterator();
}
