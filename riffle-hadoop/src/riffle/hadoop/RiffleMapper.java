package riffle.hadoop;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.*;

public class RiffleMapper extends Mapper<Writable, Writable, BytesWritable, BytesWritable> {

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
