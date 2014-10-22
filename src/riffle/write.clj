(ns riffle.write
  (:require
    [byte-streams :as bs]
    [clojure.java.io :as io]
    [riffle.data.riffle :as l]))

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
          block-size 4096}}]
     (l/write-riffle kvs x
       {:sorted? sorted?
        :compressor compressor
        :hash hash
        :checksum checksum
        :block-size block-size})))
