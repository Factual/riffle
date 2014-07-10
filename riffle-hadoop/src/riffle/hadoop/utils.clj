(ns riffle.hadoop.utils
  (:refer-clojure :exclude [partition comparator])
  (:require
    [clojure.tools.logging :as log]
    [clojure.edn :as edn]
    [primitive-math :as p]
    [byte-streams :as bs]
    [byte-transforms :as bt]
    [riffle.write :as w]
    [riffle.data.riffle :as r]
    [riffle.data.utils :as u])
  (:import
    [java.util.concurrent
     ArrayBlockingQueue]
    [org.apache.hadoop.conf
     Configuration]
    [org.apache.hadoop.fs
     FileSystem
     Path]
    [org.apache.hadoop.util
     Progressable]))

(defn set-val! [^Configuration conf k v]
  (.set conf (name k) (pr-str v)))

(defn get-val [^Configuration conf k default-val]
  (if-let [v (.get conf (name k))]
    (edn/read-string v)
    default-val))

(let [l (Math/log 2)]
  (defn log2 [n]
    (/ (Math/log n) l)))

(defn partition [^bytes k hash-fn num-partitions]
  (p/>> (p/int->uint (bt/hash k hash-fn))
    (p/- 32 (p/long (log2 num-partitions)))))

(defn comparator [hash-fn]
  (r/key-comparator #(bt/hash % hash-fn)))

(defn writer [os _ compressor block-size]
  (let [q (ArrayBlockingQueue. 1024)
        s (->> (repeatedly #(.take q))
            (take-while (complement #{::closed})))
        thunk (future
                (w/write-riffle s os
                  {:sorted? true
                   :compressor compressor
                   :block-size block-size}))
        cnt (atom 0)]
    [(fn [k v]
       (.put q [k v]))

     (fn [_]
       (.put q ::closed)
       @thunk)]))

(defn merged-kvs [shard num-shards ^FileSystem fs paths]
  (log/info 'REDUCING shard num-shards (pr-str paths))
  (let [cmp (comparator :murmur32)]
    (->> paths
      (map #(.open fs (Path. %) 1e7))
      (map r/entries)
      (apply u/merge-sort-by (fn [a b] (cmp (first a) (first b))))
      (filter #(= shard (partition (first %) :murmur32 num-shards))))))
