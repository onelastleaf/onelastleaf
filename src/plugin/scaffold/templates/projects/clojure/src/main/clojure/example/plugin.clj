(ns example.plugin
  (:require [clojure.string :as string]
            [onelastleaf.plugin-sdk :as oll]))

(defn echo [arguments]
  (string/join " " arguments))

(defn echo-action [_ arguments]
  (oll/string-result (echo arguments)))

(defn -main [& _]
  (-> (oll/plugin "__OLL_PLUGIN_ID__" "0.1.0")
      (oll/action "echo" "Return the supplied arguments" echo-action)
      (oll/run)
      (.toCompletableFuture)
      (.join)))
