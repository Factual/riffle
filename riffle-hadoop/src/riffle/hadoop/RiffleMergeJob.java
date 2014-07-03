package riffle.hadoop;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import java.io.*;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import clojure.java.api.Clojure;
import clojure.lang.IFn;


public class RiffleMergeJob {

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

        int _numPaths;
        LinkedList<PathTuple> _paths;

        public PathInputFormat(List<List<String>> paths) {
            _paths = new LinkedList();
            for (List<String> l : paths) {
                for (int i = 0; i < l.size(); i++) {
                    _paths.add(new PathTuple(i, l.get(i)));
                }
            }
            _numPaths = _paths.size();
        }

        public List<InputSplit> getSplits(JobContext jobContext) {
            List<InputSplit> ret = new ArrayList<InputSplit>();
            int numSplits = jobContext.getConfiguration(). getInt(MRJobConfig.NUM_MAPS, 1);
            for (int i = 0; i < numSplits; ++i) {
                ret.add(new EmptySplit());
            }
            return ret;
        }

        public RecordReader<IntWritable,Text> createRecordReader(InputSplit ignored, TaskAttemptContext taskContext) throws IOException {

            return new RecordReader<IntWritable,Text>() {

                public void initialize(InputSplit split, TaskAttemptContext context) { }

                public boolean nextKeyValue() {
                    if (!_paths.isEmpty()) {
                        _paths.pop();
                    }
                    return !_paths.isEmpty();
                }

                public IntWritable getCurrentKey() { return new IntWritable(_paths.peek().key); }

                public Text getCurrentValue() { return new Text(_paths.peek().value); }

                public void close() throws IOException { }

                public float getProgress() throws IOException {
                    return (float) (_numPaths - _paths.size()) / _numPaths;
                }
            };
        }
    }


}
