(ns simple-pulsar.client
  (:require [cheshire.core :as json])
  (:import [org.apache.pulsar.client.api PulsarClient]
           [org.apache.pulsar.client.api Schema]
           [java.util.concurrent TimeUnit]))

(def service-url "pulsar://localhost:6650")

(defn make-client
  [service-url]
  (-> (PulsarClient/builder)
      (.serviceUrl service-url)
      (.build)))

(defn make-producer
  [client topic]
  (-> (.newProducer client)
      (.topic topic)
      (.create)))

(defn make-producer-string
  [client topic]
  (-> (.newProducer client Schema/STRING)
      (.topic topic)
      (.create)))

(def array-of-bytes-type (Class/forName "[B"))

(defn- config-props [message props]
  (when props
    (doseq [prop props]
      (.property message (key prop) (val prop)))
    message))

(defn- message-value
  [value]
  (condp = (type value)
    array-of-bytes-type value
    String value
    (-> value
        json/generate-string
        .getBytes)))

(defn- send-message*
  [producer value]
  (let [value (message-value value)]
    (.send producer value)))

(defn send-message
  ([producer value key props]
   (let [value (message-value value)]
     (if (or key props)
       (let [msg (cond-> (.newMessage producer)
                   key (.key key)
                   value (.value value)
                   props (config-props props))]
         (.send msg))
       (send-message* producer value))))
  ([producer value]
   (send-message producer value nil nil)))

(defn- consumer-opts [consumer opts]
  (println "ack-timeout:" (:ack-timeout opts))
  (println "time-unit:" (or (:time-unit opts) TimeUnit/SECONDS))
  (cond-> consumer
    (:ack-timeout opts) (.ackTimeout (:ack-timeout opts) (or (:time-unit opts) TimeUnit/SECONDS))))

(defn make-consumer
  ([client topics subscription-name opts]
   (println "make-consumer: with opts:" opts)
   (let [topics (into-array String topics)
         consumer (cond-> (.newConsumer client)
                    topics (.topics topics)
                    subscription-name (.subscriptionName subscription-name)
                    opts (consumer-opts opts))]
     (.subscribe consumer)))
  ([client topics subscription-name]
   (make-consumer client topics subscription-name nil)))

(defn start-job
  [service-url subscription-name topics process-fn ex-fn opts]
  (with-open [client (make-client service-url)
              consumer (make-consumer client topics subscription-name opts)]
    (printf "start-job subscription: %s, topics: %s.%n" subscription-name topics)
    (try
      (while true
        (let [message (.receive consumer)]
          (process-fn message)
          (.acknowledge consumer message)))
      (catch Exception ex
        (println ex "Caught exception, processing topics:" topics)
        (if ex-fn
          (ex-fn ex)))
      (finally
        (.close consumer)
        (System/exit 1)))))