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
  (-> (data/literal-block (mhash/decode (:id doc)) (hex/decode (:blob doc)))
      (block/with-stats (:stats doc))))

(defn- opts->query
  [opts]
  (->> (cond-> {}
         (:algorithm opts) (assoc :algorithm (:algorithm opts))
         (:after opts)     (assoc :id {$regex (str "^" (:after opts))}))
       (hash-map :query)))

(defn- opts->limit
  [opts]
  (cond-> {}
    (:limit opts) (assoc :limit (:limit opts))))

(defn- execute-query
  [db collname m]
  (let [coll (.getCollection db collname)]
    (query/exec (merge {:collection coll} (query/empty-query) m))))

(defn cur->seq
  [cur]
  (lazy-seq
    (when-let [s (seq cur)]
      (cons (doc->block (first cur)) (cur->seq (rest cur))))))

(defn block->hex
  [block]
  (let [dst (java.io.ByteArrayOutputStream.)]
    (clojure.java.io/copy (block/open block) dst)
    (hex/encode (.toByteArray dst)))) 

(defrecord MongerStore
  [uri]

  store/BlockStore

  (-stat
    [this id]
    (when-let [doc (mc/find-one-as-map (:db this) "blocks" {:id (mhash/hex id)})]
      (:stats doc)))

  (-list
    [this opts]
    (-> (execute-query (:db this) "blocks" (merge (opts->query opts) (opts->limit opts)))
        (cur->seq)))

  (-get
    [this id]
    (when-let [doc (mc/find-one-as-map (:db this) "blocks" {:id (mhash/hex id)})]
      (doc->block doc)))

  (-put!
    [this block]
    (let [id (:id block)
          hex (mhash/hex id)]
      (if-let [doc (mc/find-one-as-map (:db this) "blocks" {:id hex})]
        (doc->block doc)
        (let [stats {:stored-at (java.util.Date.)
                     :size (:size block)}]
          (mc/insert (:db this)
                     "blocks"
                     {:id hex
                      :blob (block->hex block)
                      :stats stats
                      :algorithm (:algorithm id)})
          (block/with-stats block stats)))))

  (-delete!
    [this id]
    (when-let [hex (mhash/hex id)]
      (-> (mc/remove (:db this) "blocks" {:id hex})
          (.getN)
          (not= 0))))

  store/ErasableStore

  (-erase!
    [this]
    (mg/drop-db (:conn this) (.getName (:db this)))))

(defn disconnect!
  "Disconnect from MongoDB."
  [store]
  (mg/disconnect (:conn store)))

(store/privatize-constructors! MongerStore)

(defn monger-block-store
  "Creates a new Monger block store.

  Supported options:

  - `host`
  - `port`
  - `db-name`
  - `credentials`"
  [& {:as opts}]
  (map->MongerStore
    (let [conn (mg/connect opts)]
      (merge {:conn conn
              :db (mg/get-db conn (:db-name opts))}
             (dissoc opts :credentials)))))

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
