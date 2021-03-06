(ns riffle.hadoop.cli
  (:require
    [clojure.tools.cli :as cli]
    [riffle.cli :as riff]
    [clojure.java.io :as io])
  (:import
    [riffle.hadoop
     RiffleBuildJob
     RiffleBuildJob$Mapper
     RiffleBuildJob$Partitioner
     RiffleBuildJob$OutputFormat
     RiffleBuildJob$Comparator
     RiffleMergeJob
     RiffleMergeJob$PathInputFormat
     RiffleMergeJob$Partitioner
     RiffleMergeJob$Reducer]
    [org.apache.hadoop.mapreduce Job Reducer]
    [org.apache.hadoop.fs Path FileSystem FileStatus]
    [org.apache.hadoop.conf Configuration]
    [org.apache.hadoop.io BytesWritable Text LongWritable IntWritable]
    [org.apache.hadoop.mapreduce.lib.input
     FileInputFormat
     TextInputFormat]
    [org.apache.hadoop.mapreduce.lib.output
     FileOutputFormat
     TextOutputFormat])
  (:gen-class))

 (defn build-job [^Configuration conf shards srcs dst]
  (let [job (doto (Job. conf (str "build-riffle-index: "
                               (apply str (interpose "," srcs))
                               " -> " dst))
              (.setJarByClass RiffleBuildJob)
              (.setOutputKeyClass BytesWritable)
              (.setOutputValueClass BytesWritable)
              (.setInputFormatClass TextInputFormat)
              (.setOutputFormatClass RiffleBuildJob$OutputFormat)
              (.setMapperClass RiffleBuildJob$Mapper)
              (.setPartitionerClass RiffleBuildJob$Partitioner)
              (.setSortComparatorClass RiffleBuildJob$Comparator)
              (.setNumReduceTasks shards)
              (FileOutputFormat/setOutputPath (Path. dst)))]
    (doseq [src srcs]
      (FileInputFormat/addInputPath job (Path. src)))
    job))

(defn files [conf path]
  (let [fs (FileSystem/get conf)]
    (->> (.listStatus fs (Path. path))
      (map #(.getPath ^FileStatus %))
      (remove #(.startsWith (.getName ^Path %) "_"))
      (mapv str))))

(defn merge-job [^Configuration conf shards srcs dst]
  (doto (Job. conf (str "merge-riffle-indices: "
                     (apply str (interpose "," srcs))
                     " -> " dst))
    (.setJarByClass RiffleBuildJob)
    (.setOutputKeyClass BytesWritable)
    (.setOutputValueClass BytesWritable)
    (.setMapOutputKeyClass IntWritable)
    (.setMapOutputValueClass Text)
    (.setInputFormatClass RiffleMergeJob$PathInputFormat)
    (.setOutputFormatClass RiffleBuildJob$OutputFormat)
    (.setPartitionerClass RiffleMergeJob$Partitioner)
    (.setReducerClass RiffleMergeJob$Reducer)
    (.setNumReduceTasks shards)
    (FileInputFormat/addInputPath (Path. "ignore"))
    (FileOutputFormat/setOutputPath (Path. dst))
    (RiffleMergeJob/setPaths (mapv #(files conf %) srcs))))

(def options
  [["-s" "--shards SHARDS"
    :default 64
    :parse-fn #(long (Double/parseDouble %))]
   [nil "--block-size BLOCKSIZE"
    :default 8192
    :parse-fn #(long (Double/parseDouble %))]
   [nil "--compressor COMPRESSOR"
    :default "gzip"]])

(defn config? [^String s]
  (.startsWith s "-D"))

(defn config->kv [^String s]
  (let [s (.substring s 2)
        idx (.indexOf s "=")]
    [(.substring s 0 idx)
     (.substring s (inc idx))]))

(defn -main [& args]
  (let [args' (remove config? args)
        task (first args')
        {:keys [options arguments summary errors]} (cli/parse-opts (rest args') options)
        {:keys [shards block-size compressor]} options
        srcs (butlast arguments)
        dst (last arguments)

        conf (doto (Configuration.)
               (.setLong "mapreduce.task.timeout" (* 1000 60 60 6))
               (.setLong "mapred.task.timeout" (* 1000 60 60 6))
               (.setInt "riffle.shards" shards))

        _  (doseq [[k v] (->> args (filter config?) (map config->kv))]
             (.set conf k v))

        job (case task
              "build" (build-job conf shards srcs dst)
              "merge" (do
                        (.setInt conf "mapreduce.job.maps" 1)
                        (merge-job conf shards srcs dst)))]

    (doto job
      (RiffleBuildJob/setBlockSize block-size)
      (RiffleBuildJob/setCompressFunction compressor))

    (.waitForCompletion job true)

    (System/exit 0)))
