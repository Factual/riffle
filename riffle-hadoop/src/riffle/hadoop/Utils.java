package riffle.hadoop;

import org.apache.hadoop.conf.Configuration;
import clojure.java.api.Clojure;
import clojure.lang.IFn;

public class Utils {
    private static IFn _getFn;
    private static IFn _setFn;

    static {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("riffle.hadoop.utils"));

        _getFn = Clojure.var("riffle.hadoop.utils", "get-val");
        _setFn = Clojure.var("riffle.hadoop.utils", "set-val!");
    }

    public static void set(Configuration conf, String k, Object v) {
        _setFn.invoke(conf, k, v);
    }

    public static Object get(Configuration conf, String k, Object defaultVal) {
        return _getFn.invoke(conf, k, defaultVal);
    }

}
