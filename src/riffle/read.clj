(ns riffle.read
  (:refer-clojure :exclude [get])
  (:require
    [primitive-math :as p]
    [byte-streams :as bs]
    [byte-transforms :as bt]
    [clojure.java.io :as io]
    [riffle.data
     [header :as h]
     [riffle :as r]
     [block :as b]
     [utils :as u]])
  (:import
    [java.io
     IOException
     InputStream
     File]
    [riffle.data.riffle
     Riffle]))

;;;

(defn riffle
  "Returns a Riffle object that can be used with `get` and `entries`."
  [path]
  (r/riffle path))

(defn path [^Riffle r]
  (.getPath ^File (.file r)))

(defn riffle?
  "Returns true if the file at `path` is a valid Riffle file."
  [path]
  (try
    (let [{:keys [file-length]} (h/decode-header (io/file path))]
      (= (.length (io/file path)) file-length))
    (catch Throwable e
      false)))

(defn stream-entries
  "Returns a stream of entries based on a Riffle InputStream.  The InputStream will be closed
   when the end of the key/value blocks are reached."
  [^InputStream is]
  (r/entries is identity))

;;;

(defrecord RiffleSet
  [idx->riffles
   riffle->priority
   hash-fn]
  java.io.Closeable
  (close [_]
    (doseq [^Riffle r (->> idx->riffles
                        vals
                        (apply concat)
                        distinct)]
      (.close r))))

(def ^:private ^:const set-bits 10)

(defn riffle-set
  ([]
     (riffle-set :murmur32))
  ([hash]
     (RiffleSet.
       (vec (repeat (Math/pow 2 set-bits) nil))
       {}
       #(bt/hash % hash))))

(defn- index ^long [^long hash]
  (p/>> (p/int->uint hash) (p/- 32 set-bits)))

(defn- indices [^Riffle r]
  (let [mask (index (.hash-mask r))
        hash (p/bit-and mask (index (.shared-hash r)))]
    (filter
      #(p/== hash (p/bit-and (p/long %) mask))
      (range (Math/pow 2 set-bits)))))

(defn conj-riffle [^RiffleSet s ^Riffle r]
  (RiffleSet.
    (reduce
      (fn [v idx] (update-in v [idx] #(cons r %)))
      (.idx->riffles s)
      (indices r))
    (assoc (.riffle->priority s) r (->> (.riffle->priority s) vals (apply max 0) inc))
    (.hash-fn s)))

(defn disj-riffle [^RiffleSet s ^Riffle r]
  (let [r? #{r}]
    (RiffleSet.
      (mapv
        #(remove r? %)
        (.idx->riffles s))
      (dissoc (.riffle->priority s) r)
      (.hash-fn s))))

;;;

(defn entries
  "Returns a lazy sequence of 2-tuples representing keys and values.  This does not hold
   onto any file handles or other system resources, and can be safely discarded without
   being closed."
  ([r]
     (cond

       (instance? RiffleSet r)
       (let [^RiffleSet r r
             cmp (riffle.data.riffle/key-comparator (.hash-fn r))]
         (->> r
           .idx->riffles
           (apply concat)
           distinct
           (sort-by (.riffle->priority r))
           (map entries)
           (apply u/merge-sort-by (fn [[a _] [b _]] (cmp a b)))))

       (instance? Riffle r)
       (->> r
         r/block-offsets
         (map (partial r/read-block r))
         (mapcat
           (fn [block]
             (try
               (b/block->kvs block)
               (catch IOException e
                 nil)))))

       :else
       (->> r
         bs/to-input-stream
         stream-entries)))
  ([x & rst]
     (let [cmp (riffle.data.riffle/key-comparator #(bt/hash % :murmur32))]
       (->> (list* x rst)
         (map entries)
         (apply u/merge-sort-by (fn [[a _] [b _]] (cmp a b)))))))

(defn get
  "Returns the value associated with `key` as a byte-array, or `nil` if there is no such
   entry."
  [r key]
  (let [key (bs/to-byte-array key)]
    (if (instance? RiffleSet r)
      (let [^RiffleSet r r
            hash ((.hash-fn r) key)
            riffles (-> r .idx->riffles (nth (index hash)))]
        (some #(r/lookup % key hash) riffles))
     (let [^Riffle r r
           hash ((.hash-fn r) key)]
       (r/lookup r key hash)))))
