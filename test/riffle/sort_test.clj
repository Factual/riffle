(ns riffle.sort-test
  (:refer-clojure
    :exclude [sorted?])
  (:require
    [byte-streams :as bs]
    [byte-transforms :as bt]
    [riffle.data.sorted-chunk :as s]
    [clojure.java.io :as io]
    [clojure.test :refer :all]))

(def words
  (->> (io/file "test/words")
    io/reader
    line-seq))

(defn sorted? [cmp-fn s]
  (->> s
    (partition 2 1)
    (every?
      (fn [[a b]]
        (<= (cmp-fn a b) 0)))))

(deftest test-sorted-kvs
  (let [words' (shuffle words)]
    (doall
      (for [chunk-size [1e4 1e7]
            cmp-fn [bs/compare-bytes]]
        (is
          (->> (zipmap words' words')
            (s/sort-kvs cmp-fn chunk-size)
            (map first)
            (sorted? cmp-fn)))))))
