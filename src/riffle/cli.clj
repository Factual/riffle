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
    [riffle.write :as w])
  (:import
    [java.io
     FileOutputStream
     FileDescriptor]))

(def options
  [["-e" "--encode"]
   ["-d" "--delimiter DELIMITER"
    :default "\t"]
   ["-k" "--keys"]
   ["-g" "--get KEY"]
   ["-b" "--base64"]
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
      (map
        (fn [^String s]
          (let [idx (.indexOf s separator)]
            (if (neg? idx)
              [(f s) ""]
              [(f (.substring s 0 idx))
               (f (.substring s (p/+ idx len)))])))))))

(defn out []
  (FileOutputStream. FileDescriptor/out))

(defn -main [& args]
  (let [{:keys [options arguments summary errors]}
        (cli/parse-opts args options)

        {encode? :encode key :get keys? :keys base64? :base64 block-size :block-size compressor :compressor delimiter :delimiter}
        options

        files (seq arguments)

        encoder (if base64? #(bt/decode % :base64) bs/to-byte-array)
        decoder (if base64? #(bt/encode % :base64) identity)]

    (when errors
      (println "Expected arguments:\n" summary "\n")
      (doseq [e errors]
        (println e))
      (System/exit 1))

    (when-let [f (and files (some #(not (.exists (io/file %))) files))]
      (println "Invalid file:" f)
      (System/exit 1))

    (try
      (cond

        key
        (if files
          (let [rs (->> files (map r/riffle) reverse)]
            (if-let [v (some
                         #(or
                            (when base64? (r/get % (bs/to-byte-array key)))
                            (r/get % (encoder key)))
                         rs)]
              (println (bs/to-string (decoder v)))
              (do
                (println "no matching key")
                (System/exit 1)))))

        (and encode? files)
        (let [kvs (->> files
                    (map
                      (fn [f]
                        (if (r/riffle? f)
                          (r/stream-entries (bs/to-input-stream f))
                          (->> (io/file f)
                            (parse-tsv delimiter base64?)
                            (s/sort-kvs comparator 1e7)))))
                    (apply u/merge-sort-by comparator))]
          (w/write-riffle kvs (out)
            {:sorted? true
             :compressor compressor
             :block-size block-size}))

        (and encode? (not files))
        (let [kvs (parse-tsv delimiter base64? *in*)]
          (w/write-riffle kvs (out)
            {:compressor compressor
             :block-size block-size}))

        (and (not encode?) files)
        (let [f (comp bs/to-string decoder)]
          (doseq [[k v] (->> files
                          (map bs/to-input-stream)
                          (map r/stream-entries)
                          (apply u/merge-sort-by comparator))]
            (.write *out* ^String (f k))
            (when-not keys?
              (.write *out* ^String delimiter)
              (.write *out* ^String (f v)))
            (println)))

        (and (not encode?) (not files))
        (do
          (println "must specify Riffle file to decode")
          (System/exit 1)))

      (catch Throwable e
        (.printStackTrace e)
        (System/exit 1)))

    (System/exit 0)))
