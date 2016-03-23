(defproject factual/riffle "0.1.3"
  :description "a write-once key/value store"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :global-vars {*warn-on-reflection* true}
  :dependencies [[byte-transforms "0.1.4-alpha1"]
                 [byte-streams "0.2.0-alpha8"]
                 [primitive-math "0.1.4"]
                 [org.clojure/tools.cli "0.3.1"]]
  :test-selectors {:default #(not (or (:stress %) (:benchmark %)))
                   :stress :stress
                   :benchmark :benchmark}
  :profiles {:uberjar {:aot :all
                       :dependencies [[org.clojure/clojure "1.7.0"]]
                       :main riffle.cli}
             :dev {:dependencies [[org.clojure/clojure "1.7.0"]
                                  [org.clojure/test.check "0.7.0"]
                                  [criterium "0.4.3"]]
                   :main riffle.cli}}
   :codox {:include [riffle.write
                     riffle.read]
           :output-dir "doc"
           :defaults {:doc/format :markdown}
           :src-dir-uri "https://github.com/Factual/riffle/blob/master/"
           :src-linenum-anchor-prefix "L"}
   :plugins [[codox "0.8.15"]]
   :jvm-opts ["-server" "-Xmx1g"])
