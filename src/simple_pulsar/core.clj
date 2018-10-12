(ns simple-pulsar.core
  (:require [simple-pulsar.client :as pulsar]))

(defn- process-fn
  [message]
  (println "process message:" (String. (.getData message)))
  ;; (throw (ex-info "Test exception in process-fn" {:fn "process-fn"}))
  )

(defn- process-fn-with-delay
  [message]
  (println "processing message:" (String. (.getData message)))
  (println "sleep 20 seconds")
  (Thread/sleep 30000)
  (println "Done sleep"))

(defn- ex-fn
  [ex]
  (println "handle exception:" ex))

(defn -main
  [subscription-name topic]
  (pulsar/start-job "pulsar://localhost:6650" subscription-name [topic] process-fn-with-delay ex-fn {:ack-timeout 12}))