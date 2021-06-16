# Difference with upstream
* [SPARK-15777](https://issues.apache.org/jira/browse/SPARK-15777) (Partial fix) - Catalog federation
  * make ExternalCatalog configurable beyond in memory and hive
  * FileIndex for catalog tables is provided by external catalog instead of using default impl
* [SPARK-33089](https://issues.apache.org/jira/browse/SPARK-33088) Enhance ExecutorPlugin API to include callbacks on task start and end events
  * Merged upstream, remove when we migrate to 3.1  
* [SPARK-18079](https://issues.apache.org/jira/browse/SPARK-18079) - CollectLimitExec.executeToIterator should perform per-partition limits
* [SPARK-20952](https://issues.apache.org/jira/browse/SPARK-20952) - ParquetFileFormat should forward TaskContext to its forkjoinpool
* [SPARK-26626](https://issues.apache.org/jira/browse/SPARK-26626) - Limited the maximum size of repeatedly substituted aliases
  * This limits alias substitution in `CollapseProject`, `ScanOperation`, and `PhysicalOperation`. It avoids query plans growing too large after substituting aliases and collapsing projections.
* [SPARK-25200](https://issues.apache.org/jira/browse/SPARK-25200) - Allow setting HADOOP_CONF_DIR as a spark config
* SafeLogging implemented for the following files:
  * core: Broadcast, CoarseGrainedExecutorBackend, CoarseGrainedSchedulerBackend, Executor, MapOutputTracker (partial), MemoryStore, SparkContext, TorrentBroadcast
  * kubernetes: ExecutorPodsAllocator, ExecutorPodsLifecycleManager, ExecutorPodsPollingSnapshotSource, ExecutorPodsSnapshot, ExecutorPodsWatchSnapshotSource, KubernetesClusterSchedulerBackend
  * yarn: YarnClusterSchedulerBackend, YarnSchedulerBackend
* [SPARK-20001](https://issues.apache.org/jira/browse/SPARK-20001) ([SPARK-13587](https://issues.apache.org/jira/browse/SPARK-13587)) - Support PythonRunner executing inside a Conda env (and R)
* [SPARK-21195](https://issues.apache.org/jira/browse/SPARK-21195) - Automatically register new metrics from sources and wire default registry
* `spark.sql.parquet.outputTimestampType` defaults to `INT64 (TIMESTAMP_MICROS)`
* In PySpark, when populating a dataframe from rows created with named arguments, values are re-ordered
  to match the schema when the `PYSPARK_COERCE_ROWS_TO_SCHEMA` environment variable is set (#54, #59, and #61).

# Added
* [palantir/spark#381](https://github.com/palantir/spark/pull/381) Gradle plugin to easily create custom docker images for use with k8s
* [palantir/spark#521](https://github.com/palantir/spark/pull/521) K8s local file mounting
* [palantir/spark#600](https://github.com/palantir/spark/pull/600) K8s local deploy mode
* Support Arrow-serialization of Python 2 strings [(#679)](https://github.com/palantir/spark/pull/679)
  * [palantir/spark#678](https://github.com/palantir/spark/issues/678) TODO: Revert 679 once we move off of python 2
* Filter rLibDir by exists so that daemon.R references the correct file [(#460)](https://github.com/palantir/spark/pull/460)
* Add pre-installed conda configuration and use to find rlib directory [(#700)](https://github.com/palantir/spark/pull/700)  
