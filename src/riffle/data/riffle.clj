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
     InputStream
     DataInputStream
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

(defn delete-and-recreate [^File f]
  (when (.exists f)
    (.delete f))
  (.createNewFile f))

(defn write-riffle
  [kvs x {:keys [sorted? compressor hash checksum block-size]}]
  (let [compress-fn (if (= :none compressor) identity #(bt/compress % compressor))
        hash-fn #(bt/hash % hash)
        checksum-fn #(bt/hash % checksum)
        f (doto (io/file x) delete-and-recreate)
        raf (doto (RandomAccessFile. f "rw") (.seek 0))

        count (atom 0)
        blocks (->> kvs
                 (map #(do (swap! count inc) %))
                 (#(if sorted? % (s/sort-kvs (key-comparator hash-fn) 1e7 %)))
                 (b/kvs->blocks hash-fn block-size))
        slots (p/long (Math/ceil (/ @count t/load-factor)))

        table-file (u/transient-file)
        table (-> table-file
                bs/to-output-stream
                (BufferedOutputStream. 1e5)
                DataOutputStream.)]

    (.write raf
      ^bytes
      (h/encode-header
        {:file-length 0
         :version "0.1.0"
         :compressor compressor
         :hash hash
         :checksum checksum
         :count @count
         :hash-mask 0
         :shared-hash 0
         :hash-table-offset 0}))

    (loop
      [s blocks
       shared-hash (p/int (or (some-> s first :hash->offset keys first p/int->uint) 0))
       hash-mask (p/int->uint -1)
       pos 0]
      (if (empty? s)

        (let [len (+ (.length raf) 8)]

          ;; add trailer entry
          (.writeInt raf 0)
          (.writeInt raf 0)

          ;; add hash-table
          (do
            (.close table)
            (.close raf)
            (t/append-hash-table table-file f)
            (.delete table-file))

          ;; overwrite header
          (with-open [raf (RandomAccessFile. f "rw")]
            (.seek raf 0)
            (.write raf
              ^bytes
              (h/encode-header
                {:file-length (.length raf)
                 :version "0.1.0"
                 :compressor compressor
                 :hash hash
                 :checksum checksum
                 :count @count
                 :hash-mask hash-mask
                 :shared-hash shared-hash
                 :hash-table-offset len})))

          f)

        (let [{:keys [hash->offset bytes] :as block} (first s)
              hash->index (zipmap (keys hash->offset) (range))
              bytes (bs/to-byte-array (compress-fn bytes))
              len (Array/getLength bytes)
              [hash mask] (reduce
                            (fn [[hash mask] h]
                              (let [h (p/int->uint h)]
                                [(p/bit-and hash h)
                                 (p/bit-and mask (p/bit-not (p/bit-xor hash h)))]))
                            [shared-hash hash-mask]
                            (->> hash->index keys (map #(p/int->uint %))))]
          (doseq [[hash idx] hash->index]
            (t/append-entry table hash pos idx))
          (.writeInt raf (p/int (checksum-fn bytes)))
          (u/write-prefixed-array raf bytes)
          (recur (rest s) (p/int hash) (p/int mask) (p/+ pos 8 len)))))))

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
      (let [checksum' (p/int ((.checksum-fn r) block))]
        (when (p/not== checksum checksum')
          (throw (IOException. (str "bad checksum, expected " checksum " but got " checksum')))))
      (-> block ((.decompress-fn r)) bs/to-byte-array))))

(defn lookup [^Riffle r ^bytes key ^long hash]
  (when (p/== (p/bit-and (.hash-mask r) hash) (.shared-hash r))
    (when-let [[loc idx] (t/read-entry (.table r) (.table-slots r) hash)]
      (b/read-value
        (read-block r loc)
        idx
        key))))

(defn random-lookup [^Riffle r]
  (let [[loc idx] (t/random-entry (.table r) (.table-slots r))]
    (b/read-value (read-block r loc) idx nil)))

;;; block-level ops

(defn block-offsets
  ([^Riffle r]
     (block-offsets r 0))
  ([^Riffle r ^long offset]
     (lazy-seq
       (let [^BlockingQueue pool (.file-pool r)
             ^RandomAccessFile raf (.take pool)]
         (try
           (.seek raf (p/+ (.block-offset r) offset))
           (let [_ (.readInt raf)
                 len (.readInt raf)
                 offset' (p/+ offset 8 len)]
             (when (pos? len)
               (cons offset (block-offsets r offset'))))
           (finally
             (.put pool raf)))))))

(defn entries [^InputStream is kvs-filter]
  (let [header (h/decode-header is)
        f (fn this [^DataInputStream is checksum-fn decompress-fn]
            (lazy-seq
              (let [checksum (.readInt is)
                    block (u/read-prefixed-array is)]
                (when-not (zero? (Array/getLength block))
                  (concat
                    (when (p/== checksum (p/int (checksum-fn block)))
                      (try
                        (-> block decompress-fn bs/to-byte-array b/block->kvs kvs-filter)
                        (catch IOException e
                          nil)))
                    (this is checksum-fn decompress-fn))))))]
    (.skip is (- (:blocks-offset header) (:header-length header)))
    (f (DataInputStream. is)
      #(bt/hash % (:checksum header))
      #(bt/decompress % (:compressor header)))))

(defn validate [^InputStream is]
  (let [is (DataInputStream. is)
        header (h/decode-header is)
        checksum-fn #(bt/hash % (:checksum header))
        decompress-fn #(bt/decompress % (:compressor header))]
    (.skip is (- (:blocks-offset header) (:header-length header)))
    (loop [cnt 0, bad-blocks 0]
      (let [[cnt' bad-blocks']
            (try
              (let [checksum (.readInt is)
                    block (u/read-prefixed-array is)]
                (if (zero? (Array/getLength block))
                  [cnt bad-blocks]
                  (let [checksum' (p/int (checksum-fn block))]
                    (if (p/not== checksum checksum')
                      [cnt (inc bad-blocks)]
                      [(+ cnt (-> block decompress-fn bs/to-byte-array b/block->kvs clojure.core/count))
                       bad-blocks]))))
              (catch Throwable e
                [cnt (inc bad-blocks)]))]
        (if (and (= cnt cnt') (= bad-blocks bad-blocks'))
          {:count (:count header), :effective-count cnt, :bad-blocks bad-blocks}
          (recur (p/long cnt') (p/long bad-blocks')))))))
