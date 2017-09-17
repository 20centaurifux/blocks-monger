(defproject zcfux/blocks-monger "0.1.0"
  :description "MongoDB backend for blocks."
  :url "https://github.com/20centaurifux/blocks-monger"
  :license {:name "GPL3"
            :url "https://www.gnu.org/licenses/gpl-3.0.txt"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [mvxcvi/blocks "0.9.1"]
                 [com.novemberain/monger "3.1.0"]
                 [zcfux/severin-monger "0.1.0-SNAPSHOT"]
                 [org.slf4j/slf4j-nop "1.7.12"]]
  :profiles {:test {:dependencies [[org.clojure/test.check "0.9.0"]
                                   [mvxcvi/puget "1.0.1"]]}})

