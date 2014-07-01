(ns riffle.data.riffle
  (:refer-clojure :exclude [count hash])
  (:require
    [clojure.java.io :as io]
    [primitive-math :as p]
    [byte-streams :as bs]
    [byte-transforms :as bt]
    [riffle.data
     [utils :as u]
     [hash-table :as t]
     [block :as b]
     [header :as h]
     [sorted-chunk :as s]])
  (:import
    [java.util.concurrent
     BlockingQueue
     ArrayBlockingQueue]
    [java.nio
     ByteBuffer]
    [java.lang.reflect
     Array]
    [java.io
     IOException
     File
     RandomAccessFile
     OutputStream
     FileOutputStream
     FileInputStream
     BufferedOutputStream
     DataOutputStream]))

;; write

(defn key-comparator
  [hash]
  (fn [a b]
    (let [h-a (p/long (hash a))
          h-b (p/long (hash b))]
      (if (p/== h-a h-b)
        (bs/compare-bytes a b)
        (compare h-a h-b)))))

(defn kvs->riffle-parts
  [kvs
   {:keys [sorted?
           compress-fn
           hash-fn
           checksum-fn
           block-size
           chunk-size]
    :or {chunk-size 1e8}}]
  (let [^File block-file (u/transient-file)
        ^File table-file (u/transient-file)
        count (atom 0)
        blocks (->> kvs
                 (map #(do (swap! count inc) %))
                 (#(if sorted? % (s/sort-kvs (key-comparator hash-fn) chunk-size %)))
                 (b/kvs->blocks hash-fn block-size))
        slots (p/long (Math/ceil (/ @count t/load-factor)))]

    (with-open [table (RandomAccessFile. table-file "rw")]
      (doto table
        (.setLength (p/* t/slot-length slots))
        u/reset-file))

    (let [table (u/mapped-buffer table-file "rw"  nil nil)]
      (with-open [os (-> block-file
                       FileOutputStream.
                       (BufferedOutputStream. (long 1e5))
                       DataOutputStream.)]
        (loop
          [s blocks
           hash (p/int (or (some-> s first :hash->offset keys first p/int->uint) 0))
           mask (p/int->uint -1)
           pos 0]
          (if (empty? s)
            {:count @count
             :shared-hash hash
             :hash-mask mask
             :table-file table-file
             :block-file block-file}
            (let [{:keys [hash->offset bytes] :as block} (first s)
                  hash->index (zipmap (keys hash->offset) (range))
                  bytes (bs/to-byte-array (compress-fn bytes))
                  len (Array/getLength bytes)
                  [hash mask] (reduce
                                (fn [[hash mask] h]
                                  (let [h (p/int->uint h)]
                                    [(p/bit-and hash h)
                                     (p/bit-and mask (p/bit-not (p/bit-xor hash h)))]))
                                [hash mask]
                                (->> hash->index keys (map #(p/int->uint %))))]
              (doseq [[hash idx] hash->index]
                (t/write-entry table 0 slots [hash pos idx]))
              (.writeInt os (p/int (checksum-fn bytes)))
              (u/write-prefixed-array os bytes)
              (recur (rest s) (p/int hash) (p/int mask) (p/+ pos 8 len)))))))))

(defn delete-and-recreate [^File f]
  (when (.exists f)
    (.delete f))
  (.createNewFile f))

(defn write-riffle
  [kvs x {:keys [sorted? compressor hash checksum block-size]}]
  (let [{:keys [count ^File table-file ^File block-file hash-mask shared-hash]}
        (kvs->riffle-parts kvs
          {:compress-fn (if (= :none compressor) identity #(bt/compress % compressor))
           :hash-fn #(bt/hash % hash)
           :checksum-fn #(bt/hash % checksum)
           :block-size block-size
           :sorted? sorted?})
        x (if (string? x) (io/file x) x)
        _ (when (instance? File x) (delete-and-recreate x))
        os (bs/convert x OutputStream)
        header (h/encode-header
                 {:file-length (p/+ (.length block-file) (.length table-file))
                  :version "0.1.0"
                  :compressor compressor
                  :hash hash
                  :checksum checksum
                  :count count
                  :hash-mask hash-mask
                  :shared-hash shared-hash
                  :blocks-offset 0
                  :hash-table-offset (.length block-file)})]
    (bs/transfer header os {:close? false})
    (bs/transfer block-file os {:close? false})
    (.delete block-file)
    (bs/transfer table-file os {:close? true})
    (.delete table-file)

    true))

;; read

(defrecord Riffle
  [^File file
   ^ByteBuffer table
   ^BlockingQueue file-pool
   decompress-fn
   hash-fn
   checksum-fn
   ^long shared-hash
   ^long hash-mask
   ^long count
   ^long block-offset
   ^long table-slots]
  Object
  (finalize [_]
    (loop []
      (when-not (.isEmpty file-pool)
        (let [^RandomAccessFile f (.poll file-pool)]
          (.close f))))))

(defn riffle
  ([file]
     (riffle file 1))
  ([file pool-size]
     (let [file (io/file file)]
       (with-open [is (FileInputStream. file)]
         (let [{:keys [version
                       compressor
                       hash
                       checksum
                       blocks-offset
                       hash-table-offset
                       shared-hash
                       hash-mask
                       count
                       file-length] :as header}
               (h/decode-header is)

               table-len (p/- (.length file) (p/long hash-table-offset))]

           (when-not (= file-length (.length file))
             (throw (IOException. "Invalid file length")))

           (Riffle.
             file
             (u/mapped-buffer file "r" hash-table-offset table-len)
             (ArrayBlockingQueue.
               pool-size
               true
               (vec (repeatedly pool-size #(RandomAccessFile. file "r"))))
             (if (= :none compressor) identity #(bt/decompress % compressor))
             #(bt/hash % hash)
             #(bt/hash % checksum)
             shared-hash
             hash-mask
             count
             blocks-offset
             (p/div table-len t/slot-length)))))))

(defmacro with-raf [[raf riffle] & body]
  `(let [^Riffle r# ~riffle
         ^BlockingQueue pool# (.file-pool r#)
         ~(with-meta raf {:tag "java.io.RandomAccessFile"}) (.take pool#)]
     (try
       ~@body
       (finally
         (.put pool# ~raf)))))

(defn read-block [^Riffle r offset]
  (with-raf [raf r]
    (.seek raf (p/+ (p/long offset) (.block-offset r)))
    (let [checksum (.readInt raf)
          block (u/read-prefixed-array raf)]
      (when (p/not== checksum (p/int ((.checksum-fn r) block)))
        (throw (IOException. "bad checksum")))
      (-> block ((.decompress-fn r)) bs/to-byte-array))))

(defn lookup [^Riffle r ^bytes key ^long hash]
  (when (p/== (p/bit-and (.hash-mask r) hash) (.shared-hash r))
    (when-let [[loc idx] (t/read-entry (.table r) (.table-slots r) hash)]
      (b/read-value
        (read-block r loc)
        idx
        key))))

(defn block-offsets
  ([^Riffle r]
     (block-offsets r
       0
       (- (.length ^File (.file r))
         (.capacity ^ByteBuffer (.table r))
         (.block-offset r))))
  ([^Riffle r ^long offset limit]
     (when (< offset limit)
       (lazy-seq
         (let [^BlockingQueue pool (.file-pool r)
               ^RandomAccessFile raf (.take pool)]
           (try
             (.seek raf (p/+ (.block-offset r) offset))
             (let [_ (.readInt raf)
                   len (.readInt raf)
                   offset' (p/+ offset 8 len)]
               (cons offset (block-offsets r offset' limit)))
             (finally
               (.put pool raf))))))))
