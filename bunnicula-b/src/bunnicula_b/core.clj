(ns bunnicula-b.core
  (:gen-class)
(:require [bunnicula.component.connection :as connection]
            [bunnicula.component.publisher :as publisher]
            [bunnicula.protocol :as protocol]
            [bunnicula.component.monitoring :as monitoring]
            [bunnicula.component.consumer-with-retry :as consumer]
            [clojure.tools.logging :as log]
            [manifold.stream :as s]
            [manifold.bus :as bus]
            [manifold.deferred :as dfrd]
            [com.stuartsierra.component :as component])
  )

(declare sys)
(def test-bus (bus/event-bus))

(def connection (connection/create {:username "guest"
                                    :password "guest"
                                    :host "127.0.0.1"
                                    :port 5672
                                    :vhost "/"
                                    :secure? false
                                    :connection-name "ping-pong-test"}))

(defn handle_pong [_ {:keys [data] :as _payload}
                   _ {:keys [publisher] :as _component}]
  ;; put data onto the test-bus for later processing
  (println (str "b received " data))
  (bus/publish! test-bus "pong" data)
  (comment (when (= data "pong") (protocol/publish publisher "bunnicula.example.queue-a"
                                                   {:data "ping"}))))

(def consumer (consumer/create {:handler handle_pong
                                :options {:queue-name "bunnicula.example.queue-b"
                                          :consumer-threads 1}}))
(defn create-system
  []
  {:rmq-connection connection

   :monitoring (monitoring/create)
   :publisher (component/using
               (publisher/create)
               [:rmq-connection])
   :simple-consumer (component/using
                        consumer
                     [:rmq-connection :monitoring :publisher])
   :control-consumer (component/using
                      (consumer/create {:handler (fn [_ {:keys [data] :as _payload}
                                                     _ {:keys [simple-consumer] :as _component}]
                                                   (printf "Received %s\n" data)
                                                   (Thread/sleep 2000)
                                                   (when (= data "stop") (component/stop simple-consumer))
                                                   (when (= data "start") (component/start-system simple-consumer))
                                                   )
                                        :options {:queue-name "bunnicula.example.queue-cmd"
                                                  :consumer-threads 1}})
                      [:rmq-connection :monitoring :simple-consumer])})

(def sys (-> (create-system)
              (component/map->SystemMap)
              (component/start)))


;; start processing
(def pong-sub (bus/subscribe test-bus "pong"))
(s/consume #(println %) (bus/subscribe test-bus "pong"))

(def pong-agent (s/consume #(println %) (bus/subscribe test-bus "pong")))
(def pong-agent2 (s/consume #(println %) pong-sub))

(s/close! pong-sub)


(protocol/publish (:publisher sys) "bunnicula.example.queue-a" {:data "ping"})

(alter-var-root #'sys component/stop-system)
(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
