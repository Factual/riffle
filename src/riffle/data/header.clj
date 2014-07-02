(ns riffle.data.header
  (:refer-clojure :exclude [count])
  (:require
    [primitive-math :as p]
    [riffle.data.utils :as u]
    [byte-streams :as bs]
    [clojure.string :as str])
  (:import
    [java.lang.reflect
     Array]
    [java.io
     IOException
     ByteArrayOutputStream
     DataOutputStream
     DataInputStream
     InputStream
     RandomAccessFile]))


;; "rffl" ascii header
;; int64 file length
;; uint32 length of subsequent string
;; comma-delimited string with version, compressor, hash, and checksum functions
;; int64 k/v count
;; uint32 hash-intersection value
;; uint32 hash-intersection mask
;; int64 blocks offset
;; int64 table offset
(defn encode-header
  [{:keys [version
           compressor
           hash
           checksum
           count
           shared-hash
           hash-mask
           blocks-offset
           hash-table-offset
           file-length]
    :as opts}]
  (let [baos (ByteArrayOutputStream.)
        os (DataOutputStream. baos)
        descriptor (->> [version compressor hash checksum]
                     (map name)
                     (interpose ",")
                     (apply str)
                     bs/to-byte-array)
        len (Array/getLength descriptor)
        file-length (p/long file-length)
        blocks-offset (p/long blocks-offset)
        hash-table-offset (p/long hash-table-offset)
        header-length (p/+ 48 len)]
    (.write os (bs/to-byte-array "rffl"))
    (.writeLong os (p/+ file-length header-length))
    (u/write-prefixed-array os descriptor)
    (.writeLong os count)
    (.writeInt os (p/uint->int shared-hash))
    (.writeInt os (p/uint->int hash-mask))
    (.writeLong os (p/+ blocks-offset header-length))
    (.writeLong os (p/+ hash-table-offset header-length))
    (.toByteArray baos)))

(defn read-rffl [^DataInputStream is]
  (let [ary (byte-array 4)]
    (.read is ary)
    (when-not (= "rffl" (bs/to-string ary))
      (throw (IOException. "invalid header, not a riffle file")))))

(defn decode-header [x]
  (let [is (DataInputStream. (bs/to-input-stream x))
        _  (read-rffl is)
        file-length (.readLong is)
        ary (u/read-prefixed-array is)
        [version compress-fn hash-fn checksum-fn] (-> ary bs/to-string (str/split #","))
        count (.readLong is)
        shared-hash (p/int->uint (.readInt is))
        hash-mask (p/int->uint (.readInt is))
        blocks-offset (.readLong is)
        hash-table-offset (.readLong is)]
    {:header-length (+ 48 (Array/getLength ary))
     :version version
     :file-length file-length
     :count count
     :compressor (keyword compress-fn)
     :hash (keyword hash-fn)
     :checksum (keyword checksum-fn)
     :shared-hash shared-hash
     :hash-mask hash-mask
     :blocks-offset blocks-offset
     :hash-table-offset hash-table-offset}))
