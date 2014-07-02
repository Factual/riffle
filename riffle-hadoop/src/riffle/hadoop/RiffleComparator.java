package riffle.hadoop;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import clojure.java.api.Clojure;
import clojure.lang.IFn;

public class RiffleComparator extends WritableComparator {

    private static String _hashFn = "murmur32";
    private IFn _comparator;

    public static void setHashFunction(String hashFn) {
        _hashFn = hashFn;
    }

    protected RiffleComparator() {
        super(BytesWritable.class, true);

        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("riffle.hadoop.utils"));

        _comparator = (IFn) Clojure.var("riffle.hadoop.utils", "comparator").invoke(_hashFn);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Number n = (Number) _comparator.invoke(((BytesWritable)a).copyBytes(), ((BytesWritable)b).copyBytes());
        return n.intValue();
    }
}
