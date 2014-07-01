(ns riffle.data.hash-table
  "On-disk hash-table "
  (:require
    [riffle.data.utils :as u]
    [primitive-math :as p]
    [byte-transforms :as bt])
  (:import
    [java.io
     RandomAccessFile]
    [java.nio
     ByteBuffer]))

;;;

;; int32    hash
;; uint40   block offset
;; uint8    index within block
(def ^:const slot-length 10)

(def ^:const load-factor 0.8)

(defn put-uint40 [^ByteBuffer buf ^long pos ^long n]
  (.put buf pos (p/ubyte->byte (p/>> n 32)))
  (.putInt buf (p/+ pos 1) (p/uint->int n)))

(defn get-uint40 [^ByteBuffer buf ^long pos]
  (let [b (p/long (p/byte->ubyte (.get buf pos)))
        i (p/long (p/int->uint (.getInt buf (p/+ pos 1))))]
    (p/bit-or i (p/<< b 32))))

(defn write-entry [^ByteBuffer buf ^long offset ^long slots [hash location idx]]
  (let [hash (p/long (if (zero? hash) 1 hash))
        slot (p/long (mod hash slots))]

    (loop [slot slot]

      (let [loc (p/+ offset (p/* (p/rem slot slots) slot-length))
            hash' (p/long (.getInt buf loc))]

        (cond

          ;; nothing there, write away
          (p/== 0 hash')
          (do
            (.putInt buf loc (p/int hash))
            (put-uint40 buf (p/+ loc 4) location)
            (assert (= location (get-uint40 buf (p/+ loc 4))))
            (.put buf (p/+ loc 9) (p/ubyte->byte idx)))

          ;; someone already wrote, and we can assume it's a lower index
          (p/== hash hash')
          nil

          ;; move onto the next slot
          :else
          (recur (p/inc slot)))))))

(defn read-entry [^ByteBuffer buf ^long slots ^long hash]
  (let [hash (p/long (if (zero? hash) 1 hash))
        slot (p/long (mod hash slots))]
     (loop [slot slot]

       (let [loc (p/* (p/rem slot slots) slot-length)
             hash' (p/long (.getInt buf loc))]

         (cond

           ;; hit an empty slot, must not be here
           (p/== 0 hash')
           nil

           ;; found it!
           (p/== hash hash')
           [(get-uint40 buf (p/+ loc 4))
            (p/byte->ubyte (.get buf (p/+ loc 9)))]

           ;; move onto the next slot
           :else
           (recur (p/inc slot)))))))
