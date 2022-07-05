(ns bunnicula-a.core
  (:gen-class)
  (:require [bunnicula.component.connection :as rmq.connection]
            [bunnicula.component.publisher :as rmq.publisher]
            [bunnicula.protocol :as rmq]
            [bunnicula.component.monitoring :as consumer.monitoring]
            [bunnicula.component.consumer-with-retry :as rmq.consumer]
            [clojure.tools.logging :as log]
            [manifold.stream :as s]
            [manifold.bus :as bus]
            [manifold.deferred :as dfrd]
            [com.stuartsierra.component :as component]))

(def connection (rmq.connection/create {:username "guest"
                                    :password "guest"
                                    :host "127.0.0.1"
                                    :port 5672
                                    :vhost "/"
                                    :secure? false
                                    :connection-name "ping-pong-test"}))
(defn create-system
  []
  {:rmq-connection connection
   :monitoring (consumer.monitoring/create)
   :publisher (component/using
                (rmq.publisher/create)
                [:rmq-connection])
   :simple-consumer (component/using
                      (rmq.consumer/create {:handler (fn [_ {:keys [number] :as _payload}
                                                          _ {:keys [publisher] :as _component}]
                                                       (printf "Got a number %s\n" number)
                                                       (Thread/sleep 1000)
                                                       (rmq/publish publisher "bunnicula.example.queue"
                                                                    {:number (rand-int 20)})
                                                       (rmq/publish publisher "bunnicula.example.queue-b"
                                                                    {:data (rand-int 20)})
                                                       ;; if we roll 13, we fail
                                                       (if (= 13 number)
                                                         :bunnicula.consumer/retry
                                                         :bunnicula.consumer/ack))
                                            :options {:queue-name "bunnicula.example.queue"
                                                      :consumer-threads 1}})
                      [:rmq-connection :monitoring :publisher])})


(let [sys (-> (create-system)
              (component/map->SystemMap)
              (component/start))]
  (rmq/publish (:publisher sys) "bunnicula.example.queue" {:number 0}))
