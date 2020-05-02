(import spork/rpc :as rpc)
(import mansion/store :as ms)

(defn inc-port [self]
  (put self :current-port (inc (self :current-port)))
  self)

(defn- add-server [self name]
  (def store (ms/open name))
  (def functions
    @{:save (fn [self dot] (:save store dot))
      :load (fn [self id] (:load store id))
      :retrieve (fn [self &opt what opts]
                  (default what :all)
                  (default opts @{:id? true})
                  (:retrieve store what opts))})
  (def i (rpc/server functions (self :host) (self :current-port)))
  (put-in self [:servers name] @{:store store
                                 :instance i
                                 :port (self :current-port)})
  (:_inc-port self)
  self)

(defn- run [self]
  (each s (self :stores)
        (:_add-server self s))
  self)

(defn- visit [self name visitor]
  (def server (get-in self [:servers name]))
  (def v (rpc/client (self :host) (server :port) visitor))
  (update self :visitors |(array/concat $ v))
  v)

(defn- close [self]
  (each v (self :visitors) (:close v))
  (put self :visitors @[])
  (loop [[name server] :pairs (self :servers)]
    (:close (server :instance))
    (:close (server :store)))
  (put self :servers @{})
  self)

(def Reception
  @{:stores []
    :servers @{}
    :visitors @[]
    :host "localhost"
    :current-port 9000
    :run run
    :visit visit
    :close close
    :_add-server add-server
    :_inc-port inc-port})

(defn open [stores &opt port host]
  (assert (indexed? stores) (string "Stores must be an indexed collection"))
  (default host "localhost")
  (default port 9000)
  (var current-port (if (string? port) (scan-number port) port))
  (assert (number? port) (string "Port must be a nunber or string, got " (type port)))
  (def reception @{:stores stores
                   :host host
                   :current-port current-port})
  (table/setproto reception Reception)
  reception)
