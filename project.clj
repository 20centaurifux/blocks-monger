(defproject zcfux/blocks-monger "0.2.1"
  :description "MongoDB backend for blocks."
  :url "https://github.com/20centaurifux/blocks-monger"
  :license {:name "GPL3"
            :url "https://www.gnu.org/licenses/gpl-3.0.txt"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [mvxcvi/blocks "2.0.4"]
                 [mvxcvi/alphabase "2.1.0"]
                 [com.novemberain/monger "3.5.0"]
                 [zcfux/severin-monger "0.2.0"]
                 [org.slf4j/slf4j-nop "1.7.12"]]
  :profiles {:test {:dependencies [[org.clojure/test.check "1.0.0"]
                                   [mvxcvi/blocks-tests "2.0.3"]
                                   [mvxcvi/puget "1.3.1"]]}}
  :plugins [[lein-cljfmt "0.6.7"]])
