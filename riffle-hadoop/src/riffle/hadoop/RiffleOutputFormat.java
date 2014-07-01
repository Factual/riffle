package riffle.hadoop;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import java.io.*;
import clojure.java.api.Clojure;
import clojure.lang.IFn;
import java.util.List;

public class RiffleOutputFormat extends FileOutputFormat<BytesWritable, BytesWritable> {

    public RecordWriter<BytesWritable, BytesWritable> getRecordWriter(TaskAttemptContext context) throws IOException {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("riffle.hadoop.utils"));

        Configuration conf = context.getConfiguration();
        Path file = getDefaultWorkFile(context, "");
        FileSystem fs = file.getFileSystem(conf);
        OutputStream os = fs.create(file);

        IFn writer = Clojure.var("riffle.hadoop.utils", "writer");
        List fns = (List) writer.invoke(os, context);
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
