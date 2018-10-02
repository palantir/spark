# Difference with upstream

* SPARK-21195 - Automatically register new metrics from sources and wire default registry 
* SPARK-20952 - ParquetFileFormat should forward TaskContext to its forkjoinpool
* SPARK-20001 (SPARK-13587) - Support PythonRunner executing inside a Conda env (and R)
* SPARK-17059 - Allow FileFormat to specify partition pruning strategy via splits
* SPARK-24345 - Improve ParseError stop location when offending symbol is a token
* SPARK-23795 - Make AbstractLauncher#self() protected 
* SPARK-23153 - Support application dependencies in submission client's local file system
* SPARK-18079 - CollectLimitExec.executeToIterator should perform per-partition limits

* Partial change for SPARK-15777
 * make ExternalCatalog configurable beyond in memory and hive
 * FileIndex for catalog tables is provided by external catalog instead of using default impl

# Added

* Gradle plugin to easily create custom docker images for use with k8s
