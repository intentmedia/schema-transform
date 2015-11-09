(defproject com.intentmedia/schema-transform "0.0.1-SNAPSHOT"
  :description "Schema Transform is library for changing data specifications from one format to another to help build complex polyglot data systems."
  :url "https://github.com/intentmedia/schema-transform"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :scm {:name "git"
        :url  "https://github.com/intentmedia/schema-transform"}
  :dependencies [[cheshire "5.5.0"]
                 [prismatic/schema "1.0.1"]
                 [org.clojure/clojure "1.7.0"]
                 [camel-snake-kebab "0.3.2"]]
  :profiles {:uberjar {:aot :all}}
  :source-paths ["src"]
  :resource-paths ["resources"]
  :test-paths ["test"]
  :deploy-branches ["master"]
  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "v"]
                  ["deploy"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push"]])
