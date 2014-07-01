package riffle.hadoop;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import clojure.java.api.Clojure;
import clojure.lang.IFn;

public class RifflePartitioner extends Partitioner<BytesWritable, BytesWritable> {

    private static String _hashFn = "murmur32";

    private IFn _partition;

    public RifflePartitioner() {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("riffle.hadoop.utils"));

        _partition = Clojure.var("riffle.hadoop.utils", "partition");
    }

    public static void setHashFunction(String hashFn) {
        _hashFn = hashFn;
    }

    public int getPartition(BytesWritable key, BytesWritable value, int numPartitions) {
        Long p = (Long) _partition.invoke(key.getBytes(), _hashFn, numPartitions);
        System.err.println(p + " " + numPartitions);
        return (int) p.longValue();
    }
}
