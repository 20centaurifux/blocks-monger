(ns blocks.store.monger-test
  (:require
   [blocks.core :as block]
   [blocks.store.tests :as tests]
   [blocks.store.monger :refer [monger-block-store]]
   [monger.core :as monger]
   [clojure.test :refer :all])
  (:import com.mongodb.WriteConcern))

(monger/set-default-write-concern! WriteConcern/ACKNOWLEDGED)

(deftest ^:integration test-monger-store
  (tests/check-store #(let [store (monger-block-store
                                   :uri "monger://localhost/monger-store-test?fsync=true")]
                        @(block/erase! store)
                        store)))
