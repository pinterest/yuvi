# Yuvi

Yuvi is an in-memory storage engine for recent time series metrics data. It has the following features:

* Implemented in Java.
* Supports OpenTSDB metric ingestion and OpenTSDB queries.
* Uses delta-of-delta encoding from [Facebook Gorilla](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf) to store metrics data.
* Stores tag metadata in an inverted index for fast look ups during queries.
* Stores tag data and older metrics off heap to minimize GC pauses.
* Metrics data older than a few hours is rolled over.

## Architecture
Yuvi consists a chunk manager that stores the metrics data the last few  hours. 
A chunk manager manages several chunk, which consists of 2 hours of metrics data. 
Each chunk consists of a _tag store_ to store metrics metdata and a _metric store_ to store the 
metrics data. For efficiency, several chunks can also share a tag store. 
Once the data in a chunk is older than a configured amount of time, it is removed.

## Sample code for using the library

```java
import com.pinterest.yuvi.chunk.ChunkManager;

ChunkManager chunkManager = new ChunkManager("test", 1000);
chunkManager.addMetric("put metricName.cpu.util  1489647603 30 host=host1 cluster=c1");
List<TimeSeries> ts = chunkManager.query(Query.parse("metricName.cpu.util host=*"), 1489647600, 1489649600, QueryAggregation.NONE);
```
## NOTE

This project is under active development. A dev version of OpenTSDB integration code can be found at: https://github.com/mansu/opentsdb/tree/yuvi-dev
