Yuvi Benchmarks
================

Benchmarks for Yuvi store. 

Build the package:
mvn package

JMH benchmark arguments: 
java -jar target/benchmarks.jar com.pinterest.yuvi.chunk.ChunkQueryBenchmark -h 

Running a benchmark with gc profiler: 
java -jar target/benchmarks.jar com.pinterest.yuvi.chunk.ChunkQueryBenchmark -jvmArgs -DmetricsData=<metrics data file> -prof gc

Running Chunk data integrity checker:

java -cp target/benchmarks.jar -DprintErrorKey=true -DmetricsData=<metrics data file> -Xmx4G -Xms1G \
 -Dlog4j.configuration=file:///log4j.dev.properties -DtagStoreSize=1000000 \
 -XX:+HeapDumpOnOutOfMemoryError \
 com.pinterest.yuvi.chunk.ChunkDataIntegrityChecker 
