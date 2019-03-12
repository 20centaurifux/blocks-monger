(defproject zcfux/blocks-monger "0.2.0"
  :description "MongoDB backend for blocks."
  :url "https://github.com/20centaurifux/blocks-monger"
  :license {:name "GPL3"
            :url "https://www.gnu.org/licenses/gpl-3.0.txt"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [mvxcvi/blocks "2.0.1"]
                 [mvxcvi/alphabase "2.0.2"]
                 [com.novemberain/monger "3.1.0"]
                 [zcfux/severin-monger "0.1.0"]
                 [org.slf4j/slf4j-nop "1.7.12"]]
  :profiles {:test {:dependencies [[org.clojure/test.check "0.9.0"]
                                   [mvxcvi/blocks-tests "2.0.1"]
                                   [mvxcvi/puget "1.0.1"]]}}
  :plugins [[lein-cljfmt "0.6.4"]])
