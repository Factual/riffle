(ns riffle.data.utils
  (:require
    [primitive-math :as p]
    [byte-streams :as bs]
    [clojure.java.io :as io])
  (:import
    [java.util
     Collection
     PriorityQueue]
    [java.lang.reflect
     Array]
    [java.nio
     ByteBuffer
     MappedByteBuffer]
    [java.nio.channels
     FileChannel$MapMode]
    [java.io
     IOException
     File
     RandomAccessFile
     DataOutputStream
     DataInputStream]))

;;;

(defn ^File temp-file []
  (File/createTempFile "riffle" ""))

(defn ^File transient-file []
  (doto (temp-file) .deleteOnExit))

;; because the contents are "undefined" according to Java
(defn reset-file [^RandomAccessFile f]
  (.seek f 0)
  (let [ary (byte-array 1024)
        chunks (/ (.length f) (count ary))
        remainder (rem (.length f) (count ary))]
    (dotimes [_ chunks]
      (.write f ary))
    (.write f ary 0 remainder))
  (.seek f 0))

(defn resize-file [f length]
  (with-open [raf (RandomAccessFile. (io/file f) "rw")]
    (.setLength raf length)
    (reset-file raf)))

(defn ^MappedByteBuffer mapped-buffer [^File f mode offset length]
  (let [raf (RandomAccessFile. (io/file f) ^String mode)]
    (try
      (let [fc (.getChannel raf)]
        (try
          (.map fc
            (case mode
              "rw" FileChannel$MapMode/READ_WRITE
              "r" FileChannel$MapMode/READ_ONLY)
            (or offset 0)
            (or length (.length raf)))
          (finally
            (.close fc))))
      (finally
        (.close raf)))))

;;;;

;; adapted from code I wrote for the late, lamented Skuld

(deftype SeqContainer [cmp-fn ^long idx s]
  Comparable
  (equals [_ x]
    (and
      (instance? SeqContainer x)
      (p/== (.idx ^SeqContainer x) idx)
      (identical? (.s ^SeqContainer x) s)))
  (compareTo [_ x]
    (let [^SeqContainer x x
          cmp (p/long (cmp-fn (first s) (first (.s x))))]
      (if (p/zero? cmp)
        (- (compare idx (.idx x)))
        cmp))))

(defn- merge-sort-by- [cmp-fn ^PriorityQueue heap]
  (lazy-seq
    (loop [chunk-idx 0, buf (chunk-buffer 32)]
      (if (.isEmpty heap)
        (chunk-cons (chunk buf) nil)
        (let [^SeqContainer container (.poll heap)]
          (chunk-append buf (first (.s container)))
          (when-let [s' (seq (rest (.s container)))]
            (.offer heap (SeqContainer. cmp-fn (.idx container) s')))
          (let [chunk-idx' (unchecked-inc chunk-idx)]
            (if (< chunk-idx' 32)
              (recur chunk-idx' buf)
              (chunk-cons
                (chunk buf)
                (merge-sort-by- cmp-fn heap)))))))))

(defn distinct-keys [s]
  (lazy-seq
    (when-not (empty? s)
      (let [[k v] (first s)]
        (cons [k v]
          (distinct-keys
            (drop-while #(bs/bytes= k (first %)) s)))))))

(defn merge-sort-by
  "Like sorted-interleave, but takes a specific keyfn, like sort-by."
  [cmp-fn & seqs]
  (if (= 1 (count seqs))
    (first seqs)
    (distinct-keys
      (merge-sort-by-
        cmp-fn
        (PriorityQueue.
          ^Collection
          (map #(SeqContainer. cmp-fn %1 %2) (range) (remove empty? seqs)))))))

;;;

(defn write-prefixed-array [x ^bytes ary]
  (let [len (p/uint->int (Array/getLength ary))]
    (cond

      (instance? RandomAccessFile x)
      (let [^RandomAccessFile x x]
        (.writeInt x len)
        (.write x ary))

      (instance? DataOutputStream x)
      (let [^DataOutputStream x x]
        (.writeInt x len)
        (.write x ary 0 len))

      :else
      (throw (IllegalArgumentException.)))))

(defn read-prefixed-array [x]
  (cond

    (instance? RandomAccessFile x)
    (let [^RandomAccessFile x x
          len (p/int->uint (.readInt x))
          ary (byte-array len)]
      (.read x ary)
      ary)


    (instance? DataInputStream x)
    (let [^DataInputStream x x
          len (p/int->uint (.readInt x))
          ary (byte-array len)]
      (if (p/== 0 len)
        ary
        (loop [offset 0]
          (let [len (p/- len offset)
                len' (.read x ary offset len)]
            (if (p/< len' 0)
              (throw (IOException. "EOF on InputStream"))
              (if (p/== len len')
                ary
                (recur (p/+ offset len'))))))))

    :else
    (throw (IllegalArgumentException.))))

;;;

(defrecord BlockSizeSampler
  [^double mean
   ^double m2
   ^long cnt])

(defn sampler []
  (->BlockSizeSampler 0 0 0))

(defn update-sampler [^BlockSizeSampler s ^double value]
  (let [mean (.mean s)
        count (p/inc (.cnt s))
        delta (p/- value mean)
        mean  (p/+ mean (p/div delta (p/double count)))
        m2    (if (p/> count 1)
                (p/+ (.m2 s) (p/* delta (p/- value mean)))
                0)]
    (->BlockSizeSampler mean m2 count)))

(defn estimate-size ^long [^BlockSizeSampler s]
  (let [variance (if (> (.cnt s) 1)
                   (p/div (.m2 s) (p/double (p/dec (.cnt s))))
                   4096)]
    (p/long
      (p/+ (.mean s)
        (p/div
          (Math/sqrt variance)
          4.0)))))
