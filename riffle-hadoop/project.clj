(defproject factual/riffle-hadoop "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0-alpha3"]
                 [byte-transforms "0.1.3"]
                 [byte-streams "0.2.0-alpha4"]
                 [org.clojure/tools.cli "0.3.1"]
                 [factual/riffle "0.1.2"]
                 [org.clojure/tools.logging "0.3.1"]]
  :profiles {:provided {:dependencies [[org.apache.hadoop/hadoop-client "2.2.0"]]}
             :uberjar {:aot [riffle.hadoop.cli]}}
  :main riffle.hadoop.cli
  :java-source-paths ["src"]
  :javac-options ["-target" "1.6" "-source" "1.6"])
