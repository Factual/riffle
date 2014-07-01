(ns riffle.data.sorted-chunk
  "Chunks of key-value pairs for merge-sorting."
  (:refer-clojure
    :exclude [chunk])
  (:require
    [riffle.data.utils :as u]
    [primitive-math :as p]
    [byte-streams :as bs]
    [byte-transforms :as bt])
  (:import
    [java.lang.reflect
     Array]
    [java.io
     File
     IOException
     RandomAccessFile
     FileOutputStream
     FileInputStream
     DataOutputStream
     DataInputStream
     BufferedOutputStream
     BufferedInputStream]
    [java.util
     PriorityQueue]))

(defn length [^bytes ary]
  (Array/getLength ary))

(defrecord Entry
  [cmp-fn
   ^bytes k
   ^bytes v]
  Comparable
  (compareTo [_ o]
    (cmp-fn k (.k ^Entry o))))

(defrecord Chunk
  [cmp-fn
   ^long cnt
   ^long byte-size
   ^PriorityQueue q])

(defn chunk [cmp-fn]
  (Chunk. cmp-fn 0 0 (PriorityQueue.)))

(defn conj-kv!
  "Returns a chunk with the given k/v pair added."
  [^Chunk chunk k v]
  (let [k (bs/to-byte-array k)
        v (bs/to-byte-array v)
        e (Entry. (.cmp-fn chunk) k v)]
    (Chunk.
      (.cmp-fn chunk)
      (p/inc (.cnt chunk))
      (p/+ (.byte-size chunk) (Array/getLength k) (Array/getLength v))
      (doto ^PriorityQueue (.q chunk) (.add e)))))

(defn chunk->file
  "Writes out the sorted chunk to a file."
  [^Chunk chunk]
  (let [len (+ (.byte-size chunk) (* 8 (.cnt chunk)))
        ^File f (u/transient-file)
        ^PriorityQueue q (.q chunk)]
    (with-open [os (DataOutputStream.
                     (BufferedOutputStream.
                       (FileOutputStream. f)
                       (long 1e5)))]
      (loop []
        (when-let [^Entry e (.poll q)]
          (u/write-prefixed-array os (.k e))
          (u/write-prefixed-array os (.v e))
          (recur))))
    f))

(defn kvs->files
  "Takes a lazy sequence of k/v pairs, and returns a collection of files containing sorted
   chunks."
  [cmp-fn chunk-size kv-seq]
  (assert (pos? chunk-size))
  (loop [prev [], ^Chunk curr (chunk cmp-fn), s kv-seq]
    (cond
      (empty? s)
      (conj prev (chunk->file curr))

      (<= chunk-size (.byte-size curr))
      (recur (conj prev (chunk->file curr)) (chunk cmp-fn) s)

      :else
      (let [[k v] (first s)]
        (recur prev (conj-kv! curr k v) (rest s))))))

(defn file->kvs
  [^DataInputStream is available]
  (lazy-seq
    (if (zero? available)
      (do
        (.close is)
        nil)
      (try
        (let [k (u/read-prefixed-array is)
              v (u/read-prefixed-array is)]
          (cons
            [k v]
            (file->kvs is
              (- available (Array/getLength k) (Array/getLength v) 8))))
        (catch Throwable e
          (throw (IOException. "Could not read k/v pair from chunk file" e)))))))

(defn files->sorted-kvs [cmp-fn files]
  (let [streams (map
                  #(-> ^File %
                     FileInputStream.
                     (BufferedInputStream. (long 1e6))
                     DataInputStream.)
                  files)]
    (concat

      (->> streams
        (map #(file->kvs % (.available ^DataInputStream %)))
        (apply u/merge-sort-by #(cmp-fn (first %1) (first %2))))

      ;; when we're done, delete all the files
      (lazy-seq
        (doseq [^DataInputStream s streams]
          (.close s))
        (doseq [^File f files]
          (.delete f))))))

(defn sort-kvs
  "Takes an unsorted key/val sequence, and returns it in sorted order according to `cmp-fn`.
   The keys and vales must be serializable to bytes (strings, ByteBuffers, etc.).

   `chunk-size` describes how much of the serialized sequence will be kept in memory before
    being flushed to disk for future merge-sorting."
  [cmp-fn chunk-size kv-seq]
  (->> kv-seq
    (kvs->files cmp-fn chunk-size)
    (files->sorted-kvs cmp-fn)))
