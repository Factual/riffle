(ns riffle.read
  (:refer-clojure :exclude [get])
  (:require
    [byte-streams :as bs]
    [clojure.java.io :as io]
    [riffle.data.header :as h]
    [riffle.data.riffle :as r]
    [riffle.data.block :as b])
  (:import
    [riffle.data.riffle
     Riffle]))

(defn riffle [path]
  (r/riffle path))

(defn riffle? [path]
  (try
    (h/decode-header (io/file path))
    true
    (catch Throwable e
      false)))

(defn get [l k]
  (let [k (bs/to-byte-array k)
        hash ((.hash-fn ^Riffle l) k)]
    (r/lookup l k hash)))

(defn entries [^Riffle r]
  (->> r
    r/block-offsets
    (map (partial r/read-block r))
    (mapcat b/block->kvs)))
