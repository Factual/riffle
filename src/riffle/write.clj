(ns riffle.write
  (:require
    [byte-streams :as bs]
    [clojure.java.io :as io]
    [riffle.read :as r]
    [riffle.data.riffle :as l]
    [riffle.data.utils :as u]
    [byte-transforms :as bt]))

(defn write-riffle
  "Writes out a Riffle file based on a sequence of key/value pairs that can be coerced to binary data.
   `x` may either be a file path or a `java.io.File`.

   Available options:

   sorted? - whether the input is presorted according to Riffle's hash sort-order, defaults to false
   compressor - the compressor used to store blocks, defaults to :lz4, other valid options include :none, :snappy, :gzip
   hash - the hash function used, defaults to :mumur32
   checksum - the checksum function used, defaults to :crc32
   blocksize - the maximum uncompressed block size in bytes, defaults to 4096"
  ([kvs x]
     (write-riffle kvs x nil))
  ([kvs
    x
    {:keys [sorted? compressor hash checksum block-size]
     :or {sorted? false
          compressor :lz4
          hash :murmur32
          checksum :crc32
          block-size 4096}
     :as options}]
     (l/write-riffle kvs x
       {:sorted? sorted?
        :compressor compressor
        :hash hash
        :checksum checksum
        :block-size block-size})))

(defn merge-riffles
  "Merges together a collection of Riffle files into a single index, writing out to `x`, which may either be a file path
   or a `java.io.File.`  The options mirror those in `write-riffle`."
  ([merge-fn paths x]
     (merge-riffles merge-fn paths x nil))
  ([merge-fn riffles x options]
     (let [cmp (riffle.data.riffle/key-comparator #(bt/hash % :murmur32))
           kvs (->> riffles
                 (map r/riffle)
                 (map r/entries)
                 (apply u/merge-sort-with merge-fn (fn [[a _] [b _]] (cmp a b)))
                 (map (fn [[k v]] [(bs/to-byte-array k) (bs/to-byte-array v)])))]
       (write-riffle
         kvs
         x
         (assoc options :sorted? true)))))
