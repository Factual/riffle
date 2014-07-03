(ns riffle.hadoop.cli
  (:require
    [clojure.tools.cli :as cli])
  (:import
    [riffle.hadoop
     RiffleBuildJob
     RiffleBuildJob$Mapper
     RiffleBuildJob$Partitioner
     RiffleBuildJob$OutputFormat
     RiffleBuildJob$Comparator]
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
  (let [conf (doto (Configuration.)
               (.setLong "mapred.task.timeout" (* 1000 60 60 6)))
        job (doto (Job. conf "riffle")
              (.setJarByClass RiffleBuildJob)
              (.setOutputKeyClass BytesWritable)
              (.setOutputValueClass BytesWritable)
              (.setInputFormatClass TextInputFormat)
              (.setOutputFormatClass RiffleBuildJob$OutputFormat)
              (.setMapperClass RiffleBuildJob$Mapper)
              (.setPartitionerClass RiffleBuildJob$Partitioner)
              (.setSortComparatorClass RiffleBuildJob$Comparator)
              (.setNumReduceTasks 8))]

    (FileInputFormat/addInputPath job (Path. (first args)))
    (FileOutputFormat/setOutputPath job (Path. (second args)))

    (.waitForCompletion job true)

    (System/exit 0)))
