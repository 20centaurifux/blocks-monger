(ns blocks.store.monger
  "MongoDB storage backend for blocks."
  (:require (blocks
              [core :as block]
              [data :as data]
              [store :as store])
            (monger
              [core :as mg]
              [collection :as mc]
              [query :as query]
              [operators :refer :all]
              [credentials :as mcred])
            [multihash.core :as mhash]))

(defn- block->base64
  [block]
  (let [dst (java.io.ByteArrayOutputStream.)]
    (clojure.java.io/copy (block/open block) dst)
    (.encodeToString (java.util.Base64/getEncoder) (.toByteArray dst))))

(defn- base64->bytes
  [to-decode]
  (.decode (java.util.Base64/getDecoder) (.getBytes to-decode)))

(defn- doc->block
  [doc]
  (when doc
    (let [id (mhash/decode (:id doc))]
      (-> (data/literal-block id (base64->bytes (:blob doc)))
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

(defn- connect
  [store]
  (let [conn (if-let [cred (:credentials store)]
               (mg/connect-with-credentials (:host store) (:port store) cred)
               (mg/connect store))
        db (mg/get-db conn (:db-name store))]
    [conn db]))

(defmacro with-db
  [store & body]
  `(let [[conn# db#] (connect ~store)
         result# (-> db# ~@body)]
     (mg/disconnect conn#)
     result#))

(defn- doc->stats
  [doc]
  (when doc
    (assoc (:stats doc) :id (mhash/decode (:id doc)))))

(defn- cur->seq
  [cur conn]
  (lazy-seq
    (if-let [doc (first cur)]
      (cons (doc->stats doc)
            (cur->seq (rest cur) conn))
      (mg/disconnect conn))))

(defrecord MongerBlockStore
  []

  store/BlockStore

  (-stat
    [this id]
    (-> (with-db this (mc/find-one-as-map "blocks" {:id (mhash/hex id)} [:id :stats]))
        (doc->stats)))

  (-list
    [this opts]
    (let [[conn db] (connect this)]
      (-> (execute-query db "blocks" (opts->find-arg opts))
          (cur->seq conn))))

  (-get
    [this id]
    (-> (with-db this (mc/find-one-as-map "blocks" {:id (mhash/hex id)}))
        (doc->block)))

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
                         :blob (block->base64 block)
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

(defn- opts->credentials
  [opts]
  (when-let [cred (:credentials opts)]
    (let [[username password] (map cred [:username :password])]
      (mcred/create username (:db-name opts) password))))

(defn monger-block-store
  "Creates a new Monger block store.

  Supported options:

  - `host`
  - `port`
  - `db-name`
  - `credentials`"
  [& {:as opts}]
  (map->MongerBlockStore
    (cond-> opts
      (:credentials opts) (assoc :credentials (opts->credentials opts)))))

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
