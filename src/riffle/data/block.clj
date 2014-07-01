(ns riffle.data.block
  (:require
    [primitive-math :as p]
    [byte-streams :as bs]
    [riffle.data.utils :as u])
  (:import
    [java.io
     ByteArrayInputStream
     ByteArrayOutputStream
     DataInputStream
     DataOutputStream]
    [java.lang.reflect
     Array]))

(defn take-by
  [n metric s]
  (loop [curr 0, acc [], s s]
    (if (or
          (<= 255 (count acc))
          (<= n curr)
          (empty? s))
      [acc s]
      (let [x (first s)]
        (recur (p/+ curr (p/long (metric x))) (conj acc x) (rest s))))))

(defn- block->bytes [hash->offset kvs]
  (let [baos (ByteArrayOutputStream.)
        os (DataOutputStream. baos)
        table-len (+ (* 4 (count hash->offset)) 1)]

    ;; count
    (.writeByte os (p/ubyte->byte (count hash->offset)))

    ;; lookup table
    (doseq [offset (vals hash->offset)]
      (.writeInt os (p/uint->int (+ table-len offset))))

    ;; keys and vals
    (doseq [ary (apply concat kvs)]
      (u/write-prefixed-array os ary))

    (-> baos
      .toByteArray)))

(defn kvs->blocks
  [hash-fn block-size kvs]
  (let [len #(p/+ (Array/getLength (first %))
               (Array/getLength (second %))
               8)
        metric (fn [chunk]
                 (->> chunk
                   (map len)
                   (reduce +)))
        seq-fn (fn seq-fn [kv-chunks]
                 (lazy-seq
                   (when-not (empty? kv-chunks)
                     (let [[chunks rst] (take-by
                                          block-size
                                          metric
                                          kv-chunks)
                           kvs (apply concat chunks)
                           offsets (->> kvs (map len) (reductions + 0))
                           hashes (->> kvs (map first) (map hash-fn))
                           hash->offset (->> (map vector hashes offsets)
                                          (map #(apply hash-map %))
                                          (apply merge-with min)
                                          (into (sorted-map)))]
                       (cons
                         {:hash->offset hash->offset
                          :bytes (block->bytes hash->offset kvs)}
                         (seq-fn rst))))))]
    (seq-fn (partition-by (fn [[k v]] (hash-fn k)) kvs))))

(defn block->kvs
  [^bytes block]
  (let [is (-> block ByteArrayInputStream. DataInputStream.)]
    (.skip is (p/* 4 (p/byte->ubyte (.readByte is))))
    (loop [kvs []]
      (if (pos? (.available is))
        (recur (conj kvs [(u/read-prefixed-array is) (u/read-prefixed-array is)]))
        kvs))))

(defn read-value [^bytes block ^long idx k]
  (let [is (-> block ByteArrayInputStream. DataInputStream.)
        offset (p/+ 1 (p/* idx 4))
        pos (-> is (doto (.skip offset)) .readInt)]
    (.skip is (p/- pos offset 4))

    (loop []
      (if (p/zero? (.available is))
        nil
        (let [k' (u/read-prefixed-array is)
              cmp (bs/compare-bytes k k')]
          (cond
            (p/> cmp 0)
            (do
              (.skip is (p/int->uint (.readInt is)))
              (recur))

            (p/== cmp 0)
            (u/read-prefixed-array is)

            :else
            nil))))))
