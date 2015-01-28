(ns riffle.cli
  (:refer-clojure :exclude [comparator])
  (:require
    [primitive-math :as p]
    [byte-streams :as bs]
    [byte-transforms :as bt]
    [riffle.data.utils :as u]
    [riffle.data.sorted-chunk :as s]
    [clojure.tools.cli :as cli]
    [clojure.java.io :as io]
    [riffle.read :as r]
    [riffle.data.riffle :as riff]
    [riffle.write :as w])
  (:import
    [java.util
     Arrays]
    [java.lang.reflect
     Array]
    [java.util.concurrent
     ConcurrentLinkedQueue]
    [java.io
     FileInputStream
     FileOutputStream
     FileDescriptor])
  (:gen-class))

(def options
  [["-d" "--delimiter DELIMITER"
    :default "\t"]
   ["-k" "--keys"]
   ["-g" "--get KEY"]
   ["-b" "--base64"]
   ["-n" "--num-reads NUMREADS"
    :default 1e6
    :parse-fn #(long (Double/parseDouble %))]
   [nil "--block-size BLOCKSIZE"
    :default 4096
    :parse-fn #(long (Double/parseDouble %))]
   [nil "--compressor COMPRESSOR"
    :parse-fn keyword
    :default :lz4]])

(def comparator
  (riffle.data.riffle/key-comparator #(bt/hash % :murmur32)))

(defn parse-tsv [^String separator base64? x]
  (let [len (count separator)
        f (if base64?
            #(bt/decode % :base64)
            bs/to-byte-array)]
    (->> x
      bs/to-line-seq
      (remove empty?)
      (map
        (fn [^String s]
          (let [idx (.indexOf s separator)]
            (if (neg? idx)
              [(f s) ""]
              [(f (.substring s 0 idx))
               (f (.substring s (p/+ idx len)))])))))))

(defn out []
  (FileOutputStream. FileDescriptor/out))

(defn in []
  (FileInputStream. FileDescriptor/in))

;;;

(defn ->sorted-double-array [s]
  (let [ary (double-array s)]
    (Arrays/sort ^doubles ary)
    ary))

(defn lerp ^double [^double lower ^double upper ^double t]
  (+ lower (* t (- upper lower))))

(defn lerp-array ^double [^doubles ary ^double t]
  (let [len (Array/getLength ary)]
    (cond
      (== len 0) 0.0
      (== len 1) (aget ary 0)

      :else
      (if (== 1.0 t)
        (aget ary (dec len))
        (let [cnt (dec len)
              idx (* (double cnt) t)
              idx-floor (double (int idx))
              sub-t (- idx idx-floor)]
          (lerp
            (aget ary idx-floor)
            (aget ary (inc idx-floor))
            sub-t))))))

(defn quantiles [s quantiles]
  (let [ary (->sorted-double-array s)]
    (map #(lerp-array ary %) quantiles)))

(def qs [0.25 0.5 0.75 0.9 0.95 0.99 0.999])

(defn run-benchmark
  [rs num-reads concurrency]
  (let [latencies (ConcurrentLinkedQueue.)
        num-reads' (int (/ num-reads concurrency))]
    (try
      (->> (range concurrency)
       (map
         (fn [_]
           (future
             (dotimes [_ num-reads']
               (let [start (System/nanoTime)]
                 (riff/random-lookup (rand-nth rs))
                 (let [end (System/nanoTime)]
                   (.add latencies (- end start))))))))
       doall
       (map deref)
       doall)

      (map
        #(/ % 1e6)
        (.toArray latencies)))))

;;;

(defn -main [& args]
  (let [build? (= "build" (first args))
        validate? (= "validate" (first args))
        benchmark? (= "benchmark" (first args))

        {:keys [options arguments summary errors]}
        (cli/parse-opts (if (or build? validate? benchmark?) (rest args) args) options)

        {key :get keys-only? :keys base64? :base64 block-size :block-size compressor :compressor delimiter :delimiter}
        options

        files (seq arguments)

        encoder (if base64? #(bt/decode % :base64) bs/to-byte-array)
        decoder (if base64? #(bt/encode % :base64) identity)]

    (when errors
      (println "Expected arguments:\n" summary "\n")
      (doseq [e errors]
        (println e))
      (System/exit 1))

    (when-let [f (and files (first (filter #(not (.exists (io/file %))) files)))]
      (println "Invalid file:" f)
      (System/exit 1))

    (try
      (cond

        ;; benchmark reads
        benchmark?
        (do
          (assert files "must define files to benchmark")
          (flush)
          (let [{:keys [num-reads]} options
                descriptors (int (/ 1024 (count files)))
                rs (->> files (map io/file) (map #(r/riffle descriptors)) vec)]
            (dotimes [log2-readers 8]
              (let [readers (long (Math/pow 2 log2-readers))]
                (println)
                (println "with" readers (if (= 1 readers) "reader:" "readers:"))
                (let [start (System/nanoTime)
                      latencies (run-benchmark rs num-reads readers)
                      end (System/nanoTime)
                      throughput (/ num-reads (/ (- end start) 1e9))]
                  (println (format "throughput: %.2f reads/sec" throughput))
                  (println "latencies (in ms):")
                  (doseq [[q v] (map vector qs (quantiles latencies qs))]
                    (println (format "  %.1f%%  %.2f" (* q 100) v))))))))

        ;; validate the file
        validate?
        (let [{:keys [count effective-count bad-blocks blocks block-length]}
              (->> (if files
                     (map io/file files)
                     [(in)])
                (map bs/to-input-stream)
                (map riff/validate)
                (apply merge-with +))]
          (println
            (format "%d block(s), %.2f average bytes per compressed block"
              blocks
              (double (/ block-length blocks))))
          (if (and (zero? bad-blocks) (= count effective-count))
            (println "no bad blocks")
            (do
              (println
                (format "%d bad blocks, %d missing entries (%3.f%%)"
                  bad-blocks
                  (- count effective-count)
                  (float (/ (- count effective-count) count)))))))

        ;; lookup individual key
        key
        (if files
          (let [rs (->> files (map r/riffle) reverse)]
            (if-let [v (some
                         #(r/get % (encoder key))
                         rs)]
              (println (bs/to-string (decoder v)))
              (do
                (println "no matching key")
                (System/exit 1)))))

        ;; build from specified files
        (and build? files)
        (let [kvs (->> files
                    (map
                      (fn [f]
                        (if (r/riffle? f)
                          (-> f
                            io/file
                            bs/to-input-stream
                            r/stream-entries)
                          (->> (io/file f)
                            (parse-tsv delimiter base64?)
                            (s/sort-kvs comparator 1e7)))))
                    (apply u/merge-sort-by comparator))]
          (bs/transfer
            (w/write-riffle kvs (u/transient-file)
              {:sorted? true
               :compressor compressor
               :block-size block-size})
            (out)))

        ;; build from stdin
        (and build? (not files))
        (let [kvs (parse-tsv delimiter base64? (in))]
          (bs/transfer
            (w/write-riffle kvs (u/transient-file)
              {:compressor compressor
               :block-size block-size})
            (out)))

        ;; decode
        (not build?)
        (let [f (comp bs/to-string decoder)]
          (doseq [[k v] (->> (if files
                               (map io/file files)
                               [(in)])
                          (map bs/to-input-stream)
                          (map r/stream-entries)
                          (apply u/merge-sort-by comparator))]
            (.write *out* ^String (f k))
            (when-not keys-only?
              (.write *out* ^String delimiter)
              (.write *out* ^String (f v)))
            (println))))

      (catch Throwable e
        (.printStackTrace e)
        (System/exit 1)))

    (System/exit 0)))
