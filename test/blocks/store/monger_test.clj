(ns blocks.store.monger-test
  (:require
    [blocks.store.tests :as tests]
    [blocks.store.monger :refer [monger-block-store]]
    [clojure.test :refer :all]))

(deftest ^:integration test-monger-store
   (tests/check-store! #(monger-block-store :uri "monger://localhost/monger-store-test")))
