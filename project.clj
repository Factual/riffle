(defproject factual/riffle "0.1.0-SNAPSHOT"
  :description "a write-once key/value store"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :global-vars {*warn-on-reflection* true}
  :dependencies [[byte-transforms "0.1.3"]
                 [byte-streams "0.1.12-SNAPSHOT"]
                 [primitive-math "0.1.3"]
                 [org.clojure/tools.cli "0.3.1"]]
  :test-selectors {:default #(not (or (:stress %) (:benchmark %)))
                   :stress :stress
                   :benchmark :benchmark}
  :profiles {:provided {:dependencies [[org.clojure/clojure "1.6.0"]]}
             :uberjar {:aot :all}
             :dev {:dependencies [[org.clojure/clojure "1.6.0"]
                                  [org.clojure/test.check "0.5.8"]
                                  [criterium "0.4.3"]]}}
  :jvm-opts ["-server" "-Xmx1g"]
  :main riffle.cli)
