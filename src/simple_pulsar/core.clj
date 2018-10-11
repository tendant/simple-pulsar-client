(ns simple-pulsar.core
  (:require [simple-pulsar.client :as pulsar]))

(defn -main
  [subscription-name topic]
  (pulsar/start-job "pulsar://localhost:6650" subscription-name [topic] println nil nil))