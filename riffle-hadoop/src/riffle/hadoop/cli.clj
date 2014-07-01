(ns riffle.hadoop.cli
  (:require
    [clojure.tools.cli :as cli])
  (:import
    [riffle.hadoop
     RiffleMapper
     RifflePartitioner
     RiffleOutputFormat]
    [org.apache.hadoop.mapreduce Job Reducer]
    [org.apache.hadoop.fs Path]
    [org.apache.hadoop.conf Configuration]
    [org.apache.hadoop.io BytesWritable Text LongWritable]
    [org.apache.hadoop.mapreduce.lib.input
     FileInputFormat
     TextInputFormat]
    [org.apache.hadoop.mapreduce.lib.output
     FileOutputFormat])
  (:gen-class))

(defn -main [& args]
  (let [conf (Configuration.)
        job (doto (Job. conf "riffle")
              (.setJarByClass RifflePartitioner)
              (.setOutputKeyClass BytesWritable)
              (.setOutputValueClass BytesWritable)
              (.setInputFormatClass TextInputFormat)
              (.setOutputFormatClass RiffleOutputFormat)
              (.setMapperClass RiffleMapper)
              (.setPartitionerClass RifflePartitioner)
              (.setNumReduceTasks 8))]

    (FileInputFormat/addInputPath job (Path. (first args)))
    (FileOutputFormat/setOutputPath job (Path. (second args)))

    (.waitForCompletion job true)

    (System/exit 0)))
