(ns riffle.hadoop.utils
  (:refer-clojure :exclude [partition comparator])
  (:require
    [primitive-math :as p]
    [byte-streams :as bs]
    [byte-transforms :as bt]
    [riffle.write :as w]
    [riffle.data.riffle :as r])
  (:import
    [java.util.concurrent
     ArrayBlockingQueue]
    [org.apache.hadoop.util
     Progressable]))

(let [l (Math/log 2)]
  (defn log2 [n]
    (/ (Math/log n) l)))

(defn partition [^bytes k hash-fn num-partitions]
  (p/>> (p/int->uint (bt/hash k hash-fn))
    (p/- 32 (p/long (log2 num-partitions)))))

(defn comparator [hash-fn]
  (r/key-comparator #(bt/hash % hash-fn)))

(defn writer [os ^Progressable progressable compressor]
  (let [q (ArrayBlockingQueue. 1024)
        s (->> (repeatedly #(.take q))
            (take-while (complement #{::closed})))
        thunk (future (w/write-riffle s os {:sorted? true, :compressor compressor}))]
    [(fn [k v] (.put q [k v]))
     (fn [_]
       (.progress progressable)
       (.put q ::closed)
       @thunk
       (.progress progressable))]))
