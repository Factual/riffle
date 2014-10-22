(ns riffle.write
  (:require
    [byte-streams :as bs]
    [clojure.java.io :as io]
    [riffle.data.riffle :as l]))

(defn write-riffle
  "Writes out a Riffle file.  `x` may either be a file path or a `java.io.File`."
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
