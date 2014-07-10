package riffle.hadoop;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import java.io.*;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.ArrayList;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import clojure.java.api.Clojure;
import clojure.lang.IFn;


public class RiffleMergeJob {

    public static void setPaths(Job job, List<List<String>> paths) {
        Utils.set(job.getConfiguration(), "riffle.merge.paths", paths);
    }

    public static List<List<String>> getPaths(Configuration conf) {
        return (List<List<String>>) Utils.get(conf, "riffle.merge.paths", null);
    }


    // Fake "input format" that just gives generated data
    public static class PathInputFormat extends InputFormat<IntWritable,Text> {

        public static class EmptySplit extends InputSplit implements Writable {
            public void write(DataOutput out) throws IOException { }

            public void readFields(DataInput in) throws IOException { }

            public long getLength() { return 0L; }

            public String[] getLocations() { return new String[0]; }
        }

        public static class PathTuple {
            public int key;
            public String value;

            public PathTuple(int key, String value) {
                this.key = key;
                this.value = value;
            }
        }

        public PathInputFormat() {
        }

        public List<InputSplit> getSplits(JobContext jobContext) {
            List<InputSplit> ret = new ArrayList<InputSplit>();
            int numSplits = jobContext.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
            for (int i = 0; i < numSplits; ++i) {
                ret.add(new EmptySplit());
            }
            return ret;

        }

        public RecordReader<IntWritable,Text> createRecordReader(InputSplit ignored, TaskAttemptContext taskContext) throws IOException {

            return new RecordReader<IntWritable,Text>() {

                private LinkedList<PathTuple> _paths;
                private int _numPaths;
                private int _numShards;

                private IntWritable _key;
                private Text _value;

                public void initialize(InputSplit split, TaskAttemptContext context) {
                    Configuration conf = context.getConfiguration();
                    _numShards = conf.getInt("riffle.shards", 1);

                    _paths = new LinkedList();
                    for (List<String> l : getPaths(conf)) {
                        for (int i = 0; i < l.size(); i++) {
                            _paths.add(new PathTuple((int)(((float)i/l.size())*_numShards), l.get(i)));
                        }
                    }
                    _numPaths = _paths.size();
                }

                public boolean nextKeyValue() {
                    if (_paths.isEmpty()) {
                        _key = null;
                        _value = null;
                        return false;
                    }
                    _key = new IntWritable(_paths.peek().key);
                    _value = new Text(_paths.peek().value);
                    _paths.pop();
                    return true;
                }

                public IntWritable getCurrentKey() {
                    return _key;
                }

                public Text getCurrentValue() {
                    return _value;
                }

                public void close() throws IOException { }

                public float getProgress() throws IOException {
                    return (float) (_numPaths - _paths.size()) / _numPaths;
                }
            };
        }
    }

    // Partitioner
    public static class Partitioner extends org.apache.hadoop.mapreduce.Partitioner<IntWritable, Text> {

        public Partitioner() {
        }

        public int getPartition(IntWritable key, Text value, int numPartitions) {
            return key.get();
        }
    }

    //Reducer
    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, BytesWritable, BytesWritable> {

        private int _numShards;
        private IFn _mergedSeqFn;
        private FileSystem _fs;

        public Reducer() throws IOException {
            IFn require = Clojure.var("clojure.core", "require");
            require.invoke(Clojure.read("riffle.hadoop.utils"));

            _mergedSeqFn = Clojure.var("riffle.hadoop.utils", "merged-kvs");
        }

        protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException {
            _fs = FileSystem.get(context.getConfiguration());
            _numShards = context.getConfiguration().getInt("riffle.shards", 1);
        }

        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> paths = new HashSet<String>();
            for(Text val : values) {
                paths.add(val.toString());
            }

            for (List<byte[]> l : (List<List<byte[]>>)_mergedSeqFn.invoke(key.get(), new Integer(_numShards), _fs, paths)) {
                context.write(new BytesWritable(l.get(0)), new BytesWritable(l.get(1)));
            }
        }
    }
}
