(defproject factual/riffle "0.1.2-SNAPSHOT"
  :description "a write-once key/value store"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :global-vars {*warn-on-reflection* true}
  :dependencies [[byte-transforms "0.1.3"]
                 [byte-streams "0.1.13"]
                 [primitive-math "0.1.3"]
                 [org.clojure/tools.cli "0.3.1"]]
  :test-selectors {:default #(not (or (:stress %) (:benchmark %)))
                   :stress :stress
                   :benchmark :benchmark}
  :profiles {:uberjar {:aot :all
                       :dependencies [[org.clojure/clojure "1.6.0"]]
                       :main riffle.cli}
             :dev {:dependencies [[org.clojure/clojure "1.6.0"]
                                  [org.clojure/test.check "0.5.8"]
                                  [criterium "0.4.3"]]
                   :main riffle.cli}}
   :codox {:include [riffle.write
                     riffle.read]
           :output-dir "doc"}
   :plugins [[codox "0.8.10"]]
   :jvm-opts ["-server" "-Xmx1g"])
