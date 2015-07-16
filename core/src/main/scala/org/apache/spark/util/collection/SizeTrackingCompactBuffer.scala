package org.apache.spark.util.collection

import scala.reflect.ClassTag

/**
 * CompactBuffer that keeps track of its size via SizeTracker.
 */
private[spark] class SizeTrackingCompactBuffer[T: ClassTag] extends CompactBuffer[T]
  with SizeTracker {

  override def +=(t: T) = {
    super.+=(t)
    super.afterUpdate()
    this
  }

  override def ++=(t: TraversableOnce[T]) = {
    super.++=(t)
    super.afterUpdate()
    this
  }
}

private[spark] object SizeTrackingCompactBuffer {
  def apply[T: ClassTag](): SizeTrackingCompactBuffer[T] = new SizeTrackingCompactBuffer[T]

  def apply[T: ClassTag](value: T): SizeTrackingCompactBuffer[T] = {
    val buf = new SizeTrackingCompactBuffer[T]
    buf += value
  }
}
