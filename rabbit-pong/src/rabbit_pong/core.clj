(ns rabbit-pong.core
  (:gen-class)
  (:require [langohr.core      :as rmq]
            [langohr.channel   :as lch]
            [langohr.queue     :as lq]
            [langohr.exchange  :as le]
            [langohr.consumers :as lc]
            [langohr.basic     :as lb]
            [donut.system      :as ds]
            [integrant.core    :as ig]
            )
  )

(def ^{:const true}
  ping-pong-exchange "langohr")

(defn publish-action
  [ch payload routing-key ex]
  (lb/publish ch ex routing-key payload {:content-type "text/plain" :type ping-pong-exchange}))

(defn handle-ping [exchange ch q-name payload]
   (if (= (String. payload "UTF-8") "ping") (publish-action ch "pong" q-name exchange) (println "not ping")))

(defn handle-command [payload]
  (case payload
    "stop" (ds/signal ) ))

(defn start-consumer
  "Starts a consumer bound to the given topic exchange in a separate thread"
  [exchange ch topic-name queue-name handle-fn]
  (let [queue-name' (.getQueue (lq/declare ch queue-name {:exclusive false :auto-delete false}))
        handler     (fn [ch {:keys [routing-key] :as meta} ^bytes payload handle-fn]
                      (handle-fn exchange ch queue-name' payload)
                      (println (format "[consumer] Consumed '%s' from %s, routing key: %s" (String. payload "UTF-8") queue-name' routing-key))
                      )]
    (lq/bind    ch queue-name' ping-pong-exchange {:routing-key topic-name})
    (lc/subscribe ch queue-name' handler {:auto-ack true})))

(def Consumer
  #::ds{:start (fn [{{:keys [ch exchange]} ::ds/config}]
                 (le/declare ch exchange "topic" {:durable false :auto-delete false})
                 (start-consumer exchange ch "pong" "pong" handle-ping))
        :stop (fn [{{:keys [ch]} ::ds/config}]
                (lq/delete ch "pong"))
        :config {
                 :ch   (lch/open (rmq/connect))
                 :exchange ping-pong-exchange
                 }})

(def Controller
  #::ds{:start (fn [{{:keys [ch exchange]} ::ds/config}] (le/declare ch exchange "topic" {:durable false :auto-delete false}) (start-consumer exchange ch "pong-command" "pong-command-q" handle-command))
        :stop (fn [{{:keys [ch]} ::ds/config}] (lq/delete ch "pong"))
        :config {
                 :ch   (lch/open (rmq/connect))
                 :exchange ping-pong-exchange
                 }})

(def system {::ds/defs {:services {:consumer Consumer}}})

;; start the system, let it run for 5 seconds, then stop it
(comment
  (let [running-system (ds/signal system ::ds/start)]
    (Thread/sleep 15000)
    (ds/signal running-system ::ds/stop)
    (Thread/sleep 5000)
    (ds/signal running-system ::ds/start)
    (Thread/sleep 5000)
    (ds/signal running-system ::ds/stop)
    ))

(defn -main
  [& args]
  (comment (let [conn (rmq/connect)
                 ch   (lch/open conn)]
             (le/declare ch ping-pong-exchange "topic" {:durable false :auto-delete false})
             (start-consumer ch "pong" "pong")))
  (let [running-system (ds/signal system ::ds/start)])

  )

(-main)
