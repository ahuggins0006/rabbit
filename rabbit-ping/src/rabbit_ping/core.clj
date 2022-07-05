(ns rabbit-ping.core
  (:gen-class)
  (:require [langohr.core      :as rmq]
            [langohr.channel   :as lch]
            [langohr.queue     :as lq]
            [langohr.exchange  :as le]
            [langohr.consumers :as lc]
            [langohr.basic     :as lb]
            [integrant.core    :as ig]
            )
  )

(def ^{:const true}
  ping-pong-exchange "langohr")

(defn publish-action
  [ch payload routing-key ex]
  (lb/publish ch ex routing-key payload {:content-type "text/plain" :type ping-pong-exchange}))

(defn start-consumer
  "Starts a consumer bound to the given topic exchange in a separate thread"
  [ch topic-name queue-name]
  (let [queue-name' (.getQueue (lq/declare ch queue-name {:exclusive false :auto-delete false}))
        handler     (fn [ch {:keys [routing-key] :as meta} ^bytes payload]
                      (do
                        (if (= (String. payload "UTF-8") "pong") (publish-action ch "ping" "pong" ping-pong-exchange) (println "not pong"))
                        (println (format "[consumer] Consumed '%s' from %s, routing key: %s" (String. payload "UTF-8") queue-name' routing-key))))]
    (lq/bind    ch queue-name' ping-pong-exchange {:routing-key topic-name})
    (lc/subscribe ch queue-name' handler {:auto-ack true})))

(defn -main
  [& args]
  (let [conn (rmq/connect)
        ch   (lch/open conn)]
    (le/declare ch ping-pong-exchange "topic" {:durable false :auto-delete false})
    (start-consumer ch "ping" "ping")
    ;(publish-action ch "ping" "pong" ping-pong-exchange)
    )
    (Thread/sleep 2000)

  )

(-main)
