(ns blocks.store.monger
  "MongoDB storage backend for blocks."
  (:require
   [blocks.core :as block]
   [blocks.data :as data]
   [blocks.store :as store]
   [alphabase.hex :as hex]
   [manifold.stream :as s]
   [multiformats.hash :as multihash]
   [monger.collection :as mc]
   [monger.query :as query]
   [monger.operators :refer :all]
   [severin.core :as severin])
  (:use [severin.pool.monger])
  (:import blocks.data.Block))

(def pool (severin/make-pool))

(defn- block->base64
  "Reads all bytes from ` block` and returns a base64 encoded string."
  [^Block block]
  (let [dst (java.io.ByteArrayOutputStream.)]
    (block/write! block dst)
    (.encodeToString (java.util.Base64/getEncoder) (.toByteArray dst))))

(defn- base64->bytes
  "Decodes a base64 encoded string to bytes."
  [^String b64]
  (.decode (^java.util.Base64Decoder
            java.util.Base64/getDecoder)
           (.getBytes b64)))

(defn- base64->stream
  "Creates an input stream providing a decoded base64 string."
  [^String b64]
  (java.io.ByteArrayInputStream. (base64->bytes b64)))

(defn- doc->block
  "Converts a mongodb document to a block."
  ([doc]
   (-> (:id doc)
       (hex/decode)
       (multihash/decode)
       (doc->block doc)))
  ([id doc]
   (when-let [stats (:stats doc)]
     (data/create-block id
                        (:size stats)
                        (.toInstant (:stored-at stats))
                        #(base64->stream (:blob doc))))))

(defn- connect
  "Connects to the MongoDB server.
  Returns the connection and database instance."
  [store]
  (let [r (severin/create! pool (:uri store))]
    (mc/ensure-index (:db r) "blocks" (array-map :id 1))
    (mc/ensure-index (:db r) "blocks" (array-map :algorithm 1))
    r))

(defmacro with-db
  "Connects to the MongoDB server and evaluates body.
  The related database instance is inserted as second item to the form."
  [store & body]
  `(let [r# (connect ~store)
         result# (-> (:db r#) ~@body)]
     (severin/dispose! pool r#)
     result#))

(defn- execute-query
  "Executes the query `m` and returns a cursor."
  [^com.mongodb.DB db collname m]
  (let [coll (.getCollection db collname)]
    (query/exec (merge {:collection coll} m))))

(defn- cur->seq
  "Converts a MongoDB cursor to a lazy sequence.
  The connection is closed when the sequence is realized."
  [cur r]
  (lazy-seq
   (if-let [doc (first cur)]
     (cons (doc->block doc)
           (cur->seq (rest cur) r))
     (severin/dispose! pool r))))

(defn- doc->stats
  "Returns the stats from a document."
  [id doc]
  (when-let [stats (:stats doc)]
    {:id id
     :size (:size stats)
     :stored-at (.toInstant (:stored-at stats))}))

(defn- opts->query
  "Converts blocks search options to a monger query."
  [opts]
  (merge
   (query/empty-query)
   (hash-map :sort {:id 1}
             :fields [:id :stats :blob :algorithm])
   (select-keys opts [:limit])
   (->> (cond-> (select-keys opts [:algorithm])
          (:after opts) (assoc :id {$gt (:after opts)}))
        (hash-map :query))))

(defn- prepare-block
  "Prepares a new block for storage based on the given block.
  This ensures the content is loaded into memory and cleans the block
  metadata."
  [^Block block]
  (if (data/byte-content? block)
    (data/create-block
     (:id block)
     (:size block)
     (.content block))
    (data/read-block
     (:algorithm (:id block))
     (data/read-all (.content block)))))

(defrecord MongerBlockStore []
  store/BlockStore
  (-list
    [this opts]
    (let [r (connect this)]
      (-> (execute-query (:db r) "blocks" (opts->query opts))
          (cur->seq r)
          (s/->source))))

  (-stat
    [this id]
    (store/future'
     (->> (with-db this
            (mc/find-one-as-map "blocks" {:id (multihash/hex id)} [:stats]))
          (doc->stats id))))

  (-get
    [this id]
    (store/future'
     (->> (with-db this
            (mc/find-one-as-map "blocks" {:id (multihash/hex id)}))
          (doc->block id))))

  (-put!
    [this block]
    (store/future'
     (let [id (:id block)
           hex (multihash/hex id)]
       (with-db
         this
         (#(if-let [doc (mc/find-one-as-map % "blocks" {:id hex})]
             (doc->block id doc)
             (let [block' (prepare-block block)]
               (mc/insert %
                          "blocks"
                          {:id hex
                           :blob (block->base64 block')
                           :stats {:stored-at (java.util.Date/from
                                               (:stored-at block'))
                                   :size (:size block')}
                           :algorithm (:algorithm id)})
               block')))))))

  (-delete!
    [this id]
    (store/future'
     (when-let [hex (multihash/hex id)]
       (-> (with-db this (mc/remove "blocks" {:id hex}))
           (.getN)
           (not= 0)))))

  store/ErasableStore
  (-erase!
    [this]
    (store/future'
     (with-db this (mc/remove "blocks" {})))))

(defn monger-block-store
  "Creates a new Monger block store.

  Supported options:
  - `uri`"
  [& {:as opts}]
  (map->MongerBlockStore opts))

(defmethod store/initialize "monger"
  [uri]
  (monger-block-store :uri uri))
