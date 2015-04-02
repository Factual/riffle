(ns riffle.core-test
  (:require
    [clojure.test :refer :all]
    [riffle.read :as r]
    [riffle.write :as w]
    [byte-streams :as bs]
    [criterium.core :as c]
    [clojure.java.io :as io]
    [clojure.set :as set]
    [clojure.test.check
     [generators :as gen]
     [properties :as prop]
     [clojure-test :as ct :refer (defspec)]]))

(def words
  (->> (io/file "test/words")
    io/reader
    line-seq))

(defn build-dictionary []
  (when-not false #_(.exists (io/file "/tmp/dictionary_riffle"))
    (w/write-riffle (zipmap words words) "/tmp/dictionary_riffle")))

(defn dictionary []
  (r/riffle "/tmp/dictionary_riffle"))

(deftest test-dictionary
  (build-dictionary)
  (let [d (dictionary)]
    (doseq [w words]
      (is (= w (bs/to-string (r/get d w)))))
    (let [entries (r/entries d)
          ks (->> entries (map first) (map bs/to-string))
          vs (->> entries (map second) (map bs/to-string))]
      (is (= (set words) (set ks) (set vs))))))

;;;

(defn equivalent? [r m]
  (and
    (if (vector? r)
      true
      (every?
        (fn [[k v]]
          (= v (bs/to-string (r/get r k))))
        m))

    (= (set m)
      (->> (if (vector? r)
             (apply r/entries r)
             (r/entries r))
        (map (fn [[k v]]
               [(bs/to-string k)
                (bs/to-string v)]))
        set))))

(def roundtrip-merge-prop
  (prop/for-all
    [a (gen/map gen/string-ascii gen/string-ascii)
     b (gen/map gen/string-ascii gen/string-ascii)]

    (w/write-riffle a "/tmp/check-riffle-a")
    (w/write-riffle b "/tmp/check-riffle-b")

    (let [m (merge a b)]
      (and
        (equivalent?
          [(-> (r/riffle-set)
             (r/conj-riffle (r/riffle "/tmp/check-riffle-a"))
             (r/conj-riffle (r/riffle "/tmp/check-riffle-b")))
           (-> (r/riffle-set)
             (r/conj-riffle (r/mapped-riffle "/tmp/check-riffle-a"))
             (r/conj-riffle (r/mapped-riffle "/tmp/check-riffle-b")))]
          m)
        (equivalent?
          [(r/riffle "/tmp/check-riffle-a")
           (r/riffle "/tmp/check-riffle-b")
           (r/mapped-riffle "/tmp/check-riffle-a")
           (r/mapped-riffle "/tmp/check-riffle-b")]
          m)))))

(let [merge-fn (fn [a b]
                 (str
                   (+
                     (read-string (bs/to-string a))
                     (read-string (bs/to-string b)))))]
  (def roundtrip-merge-with-prop
    (prop/for-all
      [a (gen/map gen/string-ascii (gen/fmap str gen/int))
       b (gen/map gen/string-ascii (gen/fmap str gen/int))]

      (w/write-riffle a "/tmp/check-riffle-a")
      (w/write-riffle b "/tmp/check-riffle-b")
      (w/merge-riffles merge-fn ["/tmp/check-riffle-a" "/tmp/check-riffle-b"] "/tmp/check-riffle-c")

      (let [m (merge-with merge-fn a b)]
        (equivalent?
          [(r/riffle "/tmp/check-riffle-c")
           (r/mapped-riffle "/tmp/check-riffle-c")]
          m)))))

(def roundtrip-prop
  (prop/for-all
    [m (gen/map gen/string-ascii gen/string-ascii)]

    (w/write-riffle m "/tmp/check-riffle")

    (equivalent?
      [(r/riffle "/tmp/check-riffle")
       (r/mapped-riffle "/tmp/check-riffle")]
      m)))

(defspec check-roundtrip 1e2
  roundtrip-prop)

(defspec check-roundtrip-merge 1e2
  roundtrip-merge-prop)

(defspec check-roundtrip-merge-with 1e2
  roundtrip-merge-with-prop)

(defspec ^:stress stress-roundtrip 1e5
  roundtrip-prop)

(defspec ^:stress stress-roundtrip-merge 1e4
  roundtrip-merge-prop)

(defspec ^:stress stress-roundtrip-merge-with 1e4
  roundtrip-merge-with-prop)

;;;

(defn run-lookup-benchmark [keys & files]
  (let [rs (mapv r/riffle files)]
    (time
      (doseq [k keys]
        (r/get (rand-nth rs) k)))))

(deftest ^:benchmark benchmark-lookup
  (build-dictionary)
  (let [words (->> words shuffle (take 1e3))]
    (let [l (dictionary)]
      (c/quick-bench
        (doseq [w words]
          (r/get l w))))))
