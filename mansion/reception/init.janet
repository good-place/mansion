(import spork/rpc :as rpc)
(import mansion/buffet :as mb)

(defn- add-buffet [self name]
  (def buffet (mb/open name))
  (put-in self [:open-buffets name] buffet))

(defn- run [self]
  (each s (self :buffets)
        (:_add-buffet self s))
  (defn get-buffet [name]
    (get-in self [:open-buffets name]))
  (def functions
    @{:buffets (fn [self] (self :open-buffers))
      :load (fn [self buffet id]
              (:load (get-buffet buffet) id))
      :save (fn [self buffet dot] (:save (get-buffet buffet) dot))
      :retrieve (fn [self buffet &opt what opts]
                  (default what :all)
                  (default opts @{:id? true})
                  (:retrieve (get-buffet buffet) what opts))})
  (rpc/server functions (self :host) (self :port))
  self)

(defn- visit [self visitor]
  (def v (rpc/client (self :host) (self :port) visitor))
  (update self :visitors |(array/concat $ v))
  v)

(defn- close [self]
  (each v (self :visitors) (:close v))
  (put self :visitors @[])
  (loop [[name server] :pairs (self :servers)]
    (:close (server :instance))
    (:close (server :buffet)))
  (put self :servers @{})
  self)

(def Reception
  @{:buffets []
    :open-buffets @{}
    :visitors @[]
    :host "localhost"
    :current-port 9000
    :run run
    # :call call
    :visit visit
    :close close
    :_add-buffet add-buffet})

(defn open [buffets &opt port host]
  (assert (indexed? buffets) (string "Stores must be an indexed collection"))
  (default host "localhost")
  (default port 9000)
  (var current-port (if (string? port) (scan-number port) port))
  (assert (number? port) (string "Port must be a nunber or string, got " (type port)))
  (def reception @{:buffets buffets
                   :host host
                   :current-port current-port})
  (table/setproto reception Reception)
  reception)
