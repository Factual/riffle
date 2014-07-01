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

(def roundtrip-prop
  (prop/for-all
    [m (gen/map gen/string-ascii gen/string-ascii)]

    (w/write-riffle m "/tmp/check-riffle")

    (let [r (r/riffle "/tmp/check-riffle")]

      (and
        (every?
          (fn [[k v]]
            (= v (bs/to-string (r/get r k))))
          m)

        (= (set m)
          (->> (r/entries r)
            (map (fn [[k v]]
                   [(bs/to-string k)
                    (bs/to-string v)]))
            set))))))

(defspec check-roundtrip 1e2
  roundtrip-prop)

(defspec ^:stress stress-roundtrip 1e5
  roundtrip-prop)

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
