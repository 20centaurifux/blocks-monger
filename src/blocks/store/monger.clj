(ns blocks.store.monger
  "MongoDB storage backend for blocks."
  (:require [blocks.core :as block]
            [blocks.data :as data]
            [blocks.store :as store]
            [multihash.core :as mhash]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.operators :refer :all]
            [monger.query :as query]
            [alphabase.hex :as hex]))

(defn- doc->block
  [doc]
  (when doc
    (let [id (mhash/decode (:id doc))]
      (-> (data/literal-block id (hex/decode (:blob doc)))
          (block/with-stats (merge {:id id} (:stats doc)))))))

(defn- opts->query
  [opts]
  (->> (cond-> (select-keys opts [:algorithm])
         (:after opts) (assoc :id {$gt (:after opts)}))
       (hash-map :query)))

(defn- opts->limit
  [opts]
  (select-keys opts [:limit]))

(defn- opts->find-arg
  [opts]
  (merge
    (query/empty-query)
    (opts->query opts)
    (opts->limit opts)
    {:sort {:id 1}
     :fields [:id :stats]}))

(defn- execute-query
  [db collname m]
  (let [coll (.getCollection db collname)]
    (query/exec (merge {:collection coll} m))))

(defn block->hex
  [block]
  (let [dst (java.io.ByteArrayOutputStream.)]
    (clojure.java.io/copy (block/open block) dst)
    (hex/encode (.toByteArray dst)))) 

(defn connect
  [store]
  (let [conn (mg/connect store)
        db (mg/get-db conn (:db-name store))]
    [conn db]))

(defmacro with-db
  [store & body]
  `(let [conn# (mg/connect ~store)
         result# (-> (mg/get-db conn# (:db-name ~store))
                     ~@body)]
     (mg/disconnect conn#)
     result#))

(defn- cur->seq


(defrecord MongerBlockStore
  []

  store/BlockStore

  (-stat
    [this id]
    (when-let [doc (with-db this (mc/find-one-as-map "blocks" {:id (mhash/hex id)}))]
      (-> (doc->block doc)
          (block/meta-stats))))

  (-list
    [this opts]
    (let [[conn db] (connect this)
          stats (->> (execute-query db "blocks" (opts->find-arg opts))
                     (map #(assoc (:stats %) :id (mhash/decode (:id %))))
                     (doall))]
      (mg/disconnect conn)
      stats))

  (-get
    [this id]
    (when-let [doc (with-db this (mc/find-one-as-map "blocks" {:id (mhash/hex id)}))]
      (doc->block doc)))

  (-put!
    [this block]
    (let [id (:id block)
          hex (mhash/hex id)]
      (with-db
        this
        (#(if-let [doc (mc/find-one-as-map % "blocks" {:id hex})]
           (doc->block doc)
           (let [stats {:stored-at (java.util.Date.)
                        :size (:size block)}]
             (mc/insert %
                        "blocks"
                        {:id hex
                         :blob (block->hex block)
                         :stats stats
                         :algorithm (:algorithm id)})
             (block/with-stats block stats)))))))

  (-delete!
    [this id]
    (when-let [hex (mhash/hex id)]
      (-> (with-db this (mc/remove "blocks" {:id hex}))
          (.getN)
          (not= 0))))

  store/ErasableStore

  (-erase!
    [this]
    (with-db this (mc/remove "blocks" {}))))

(store/privatize-constructors! MongerBlockStore)

(defn monger-block-store
  "Creates a new Monger block store.

  Supported options:

  - `host`
  - `port`
  - `db-name`
  - `credentials`"
  [& {:as opts}]
  (map->MongerBlockStore opts))

(defmethod store/initialize "monger"
  [location]
  (let [uri (store/parse-uri location)]
    (monger-block-store
     :host (:host uri)
     :port (:or (:port uri) 27017)
     :db-name (subs (:path uri) 1)
     :credentials (when-let
                    [cred (:user-info uri)]
                    {:username (:id cred)
                     :password (:secret cred)}))))
