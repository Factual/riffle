package riffle.hadoop;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;
import java.io.*;
import java.util.List;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.io.*;
import clojure.java.api.Clojure;
import clojure.lang.IFn;

public class RiffleBuildJob {

    //

    private static String _hashFn;
    private static String _compressFn;
    private static int _blockSize = -1;

    public static void setHashFunction(Job job, String hashFn) {
        Utils.set(job.getConfiguration(), "riffle.hashfn", hashFn);
    }

    public static String getHashFunction(Configuration conf) {
        if (_hashFn == null) {
            _hashFn = (String)Utils.get(conf, "riffle.hashfn", "murmur32");
        }
        return _hashFn;
    }

    public static void setCompressFunction(Job job, String compressFn) {
        Utils.set(job.getConfiguration(), "riffle.compressfn", compressFn);
    }

    public static String getCompressFunction(Configuration conf) {
        if (_compressFn == null) {
            _compressFn = (String)Utils.get(conf, "riffle.compressfn", "gzip");
        }
        return _compressFn;
    }

    public static void setBlockSize(Job job, int blockSize) {
        Utils.set(job.getConfiguration(), "riffle.block-size", blockSize);
    }

    public static int getBlockSize(Configuration conf) {
        if (_blockSize == -1) {
            _blockSize = ((Number)Utils.get(conf, "riffle.block-size", 8192)).intValue();
        }
        return _blockSize;
    }

    // Partitioner
    public static class Partitioner extends org.apache.hadoop.mapreduce.Partitioner<BytesWritable, BytesWritable> implements Configurable {

        private IFn _partition;
        private Configuration _conf;

        public Partitioner() {
            IFn require = Clojure.var("clojure.core", "require");
            require.invoke(Clojure.read("riffle.hadoop.utils"));

            _partition = Clojure.var("riffle.hadoop.utils", "partition");
        }

        public void setConf(Configuration conf) {
            _conf = conf;
        }

        public Configuration getConf() {
            return _conf;
        }

        public int getPartition(BytesWritable key, BytesWritable value, int numPartitions) {
            Long p = (Long) _partition.invoke(key.copyBytes(), getHashFunction(_conf), numPartitions);
            return (int) p.longValue();
        }
    }

    // Comparator
    public static class Comparator extends WritableComparator implements Configurable {

        private IFn _comparator;
        private Configuration _conf;

        protected Comparator() {
            super(BytesWritable.class, true);

            IFn require = Clojure.var("clojure.core", "require");
            require.invoke(Clojure.read("riffle.hadoop.utils"));
        }

        public void setConf(Configuration conf) {
            _conf = conf;
            _comparator = (IFn) Clojure.var("riffle.hadoop.utils", "comparator").invoke(getHashFunction(_conf));
        }

        public Configuration getConf() {
            return _conf;
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Number n = (Number) _comparator.invoke(((BytesWritable)a).copyBytes(), ((BytesWritable)b).copyBytes());
            return n.intValue();
        }
    }

    // OutputFormat
    public static class OutputFormat extends FileOutputFormat<BytesWritable, BytesWritable> {

        public RecordWriter<BytesWritable, BytesWritable> getRecordWriter(TaskAttemptContext context) throws IOException {
            IFn require = Clojure.var("clojure.core", "require");
            require.invoke(Clojure.read("riffle.hadoop.utils"));

            Configuration conf = context.getConfiguration();
            Path file = getDefaultWorkFile(context, "");
            FileSystem fs = file.getFileSystem(conf);
            OutputStream os = fs.create(file);

            IFn writer = Clojure.var("riffle.hadoop.utils", "writer");
            List fns = (List) writer.invoke(os, context, getCompressFunction(conf), new Integer(getBlockSize(conf)));
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
