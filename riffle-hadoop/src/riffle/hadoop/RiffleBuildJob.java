package riffle.hadoop;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import java.io.*;
import java.util.List;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.io.*;
import clojure.java.api.Clojure;
import clojure.lang.IFn;

public class RiffleBuildJob {

    // Partitioner
    public static class Partitioner extends org.apache.hadoop.mapreduce.Partitioner<BytesWritable, BytesWritable> {

        private static String _hashFn = "murmur32";

        private IFn _partition;

        public Partitioner() {
            IFn require = Clojure.var("clojure.core", "require");
            require.invoke(Clojure.read("riffle.hadoop.utils"));

            _partition = Clojure.var("riffle.hadoop.utils", "partition");
        }

        public static void setHashFunction(String hashFn) {
            _hashFn = hashFn;
        }

        public int getPartition(BytesWritable key, BytesWritable value, int numPartitions) {
            Long p = (Long) _partition.invoke(key.copyBytes(), _hashFn, numPartitions);
            System.err.println(p + " " + numPartitions);
            return (int) p.longValue();
        }
    }

    // Comparator
    public static class Comparator extends WritableComparator {

        private static String _hashFn = "murmur32";
        private IFn _comparator;

        public static void setHashFunction(String hashFn) {
            _hashFn = hashFn;
        }

        protected Comparator() {
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

    // OutputFormat
    public static class OutputFormat extends FileOutputFormat<BytesWritable, BytesWritable> {

        private static String _compressor = "gzip";

        public static void setCompressor(String compressor) {
            _compressor = compressor;
        }

        public RecordWriter<BytesWritable, BytesWritable> getRecordWriter(TaskAttemptContext context) throws IOException {
            IFn require = Clojure.var("clojure.core", "require");
            require.invoke(Clojure.read("riffle.hadoop.utils"));

            Configuration conf = context.getConfiguration();
            Path file = getDefaultWorkFile(context, "");
            FileSystem fs = file.getFileSystem(conf);
            OutputStream os = fs.create(file);

            IFn writer = Clojure.var("riffle.hadoop.utils", "writer");
            List fns = (List) writer.invoke(os, context, _compressor);
            final IFn write = (IFn) fns.get(0);
            final IFn close = (IFn) fns.get(1);

            return new RecordWriter<BytesWritable, BytesWritable>() {
                public void close(TaskAttemptContext context) {
                    close.invoke(context);
                }

                public void write(BytesWritable k, BytesWritable v) {
                    write.invoke(k.copyBytes(), v.copyBytes());
                }
            };
        }
    }

    // Mapper
    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Writable, Writable, BytesWritable, BytesWritable> {

        protected void map(Writable key, Writable value, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
            if (value instanceof Text) {
                String s = ((Text)value).toString();
                int idx = s.indexOf("\t");
                context.write(new BytesWritable(s.substring(0, idx).getBytes("UTF-8")),
                              new BytesWritable(s.substring(idx+1).getBytes("UTF-8")));

            } else if (key instanceof BytesWritable && value instanceof BytesWritable) {
                context.write(key, value);
            } else {
                throw new IllegalArgumentException("Invalid format for RiffleMapper: " + key.getClass() + ", " + value.getClass());
            }
        }
    }



}
