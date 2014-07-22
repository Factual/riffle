(ns riffle.data.hash-table
  "On-disk hash-table "
  (:require
    [riffle.data.utils :as u]
    [primitive-math :as p]
    [byte-transforms :as bt]
    [clojure.java.io :as io]
    [byte-streams :as bs])
  (:import
    [java.io
     RandomAccessFile
     DataOutputStream
     DataInputStream
     BufferedInputStream]
    [java.nio
     ByteBuffer]))

;;;

;; int32    hash
;; uint40   block offset
;; uint8    index within block
(def ^:const slot-length 10)

(def ^:const load-factor 0.8)

(defn split-uint40 [^long n]
  [(p/ubyte->byte (p/>> n 32)) (p/uint->int n)])

(defn merge-uint40 ^long [^long uint8 ^long uint32]
  (let [b (-> uint8 p/byte p/byte->ubyte p/long)
        i (-> uint32 p/int p/int->uint p/long)]
    (p/bit-or i (p/<< b 32))))

(defn put-uint40 [^ByteBuffer buf ^long pos ^long n]
  (let [[b i] (split-uint40 n)]
    (.put buf pos (p/byte b))
    (.putInt buf (p/inc pos) (p/int i))))

(defn get-uint40 ^long [^ByteBuffer buf ^long pos]
  (merge-uint40 (.get buf pos) (.getInt buf (p/inc pos))))

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

(defn random-entry [^ByteBuffer buf ^long slots]
  (let [idx (p/long (rand-int slots))
        loc (p/* idx slot-length)]
    [(get-uint40 buf (p/+ loc 4))
     (p/byte->ubyte (.get buf (p/+ loc 9)))]))

(defn append-entry [^DataOutputStream os ^long hash ^long position ^long idx]
  (.writeInt os hash)
  (let [[b i] (split-uint40 position)]
    (.writeByte os b)
    (.writeInt os i))
  (.writeByte os (p/ubyte->byte idx)))

(defn entries [^DataInputStream is]
  (take-while (complement #{::closed})
    (repeatedly
      (fn []
        (if (pos? (.available is))
          [(.readInt is)
           (merge-uint40 (.readByte is) (.readInt is))
           (p/byte->ubyte (.readByte is))]
          (do
            (.close is)
            ::closed))))))

(defn append-hash-table [hash-entries-file riffle-file]
  (let [in (io/file hash-entries-file)
        out (io/file riffle-file)
        cnt (/ (.length in) slot-length)
        slots (Math/max 1 (long (Math/ceil (/ cnt load-factor))))
        offset (.length out)
        len (* slots slot-length)
        _ (with-open [raf (RandomAccessFile. out "rw")]
            (.setLength raf (+ offset len)))
        buf (u/mapped-buffer out "rw" offset len)
        s (-> in bs/to-input-stream (BufferedInputStream. 1e5) DataInputStream. entries)]
    (doseq [[hash p idx] s]
      (write-entry buf 0 slots [hash p idx]))
    (.force buf)
    nil))
