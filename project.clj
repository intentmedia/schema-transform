(defproject com.intentmedia/schema-transform "0.0.1-SNAPSHOT"
  :dependencies [[cheshire "5.5.0"]
                 [prismatic/schema "1.0.0"]
                 [org.clojure/clojure "1.7.0"]]
  :profiles {:uberjar {:aot :all}}
  :source-paths ["src"]
  :test-paths ["test"])