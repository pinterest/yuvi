package com.pinterest.yuvi.chunk;

/**
 * This information is exported by each chunk so that the collector is able to schedule queries that
 * span across multiple chunks. If a chunk server contains data for a particular data set, the
 * server will contain exactly one chunk object that describes which partitions are being served by
 * that chunk server. The chunk server will export exactly one ChunkInfo object for that data set.
 */
public class ChunkInfo {

  /**
   * A unique identifier for a the data set. The data set corresponds to a logical data source.
   * For example, system metrics, or user analytics. The data set may be aggregated by a variety
   * of systems, such as kafka and spark. The keyspace of the data set is the set of all metric
   * names (including the tags) from that data source.
   */
  public final String dataSet;

  public final long startTimeSecs;

  public final long endTimeSecs;

  public ChunkInfo(String dataSet, long startTimeSecs, long endTimeSecs) {
    if (dataSet == null || dataSet.isEmpty()) {
      throw new IllegalArgumentException("Invalid chunk dataset name " + dataSet);
    }
    this.dataSet = dataSet;
    this.startTimeSecs = startTimeSecs;
    this.endTimeSecs = endTimeSecs;
  }

  @Override
  public String toString() {
    return "ChunkInfo{" +
        "dataSet='" + dataSet + '\'' +
        ", startTimeSecs=" + startTimeSecs +
        ", endTimeSecs=" + endTimeSecs +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ChunkInfo chunkInfo = (ChunkInfo) o;

    if (startTimeSecs != chunkInfo.startTimeSecs) {
      return false;
    }
    if (endTimeSecs != chunkInfo.endTimeSecs) {
      return false;
    }
    return dataSet.equals(chunkInfo.dataSet);
  }

  @Override
  public int hashCode() {
    int result = dataSet.hashCode();
    result = 31 * result + (int) (startTimeSecs ^ (startTimeSecs >>> 32));
    result = 31 * result + (int) (endTimeSecs ^ (endTimeSecs >>> 32));
    return result;
  }
}
