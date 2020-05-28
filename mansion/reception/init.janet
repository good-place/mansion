(import spork/rpc :as rpc)
(import mansion/buffet :as mb)

(defn inc-port [self]
  (put self :current-port (inc (self :current-port)))
  self)

(defn- add-server [self name]
  (def buffet (mb/open name))
  (def functions
    @{:save (fn [self dot] (:save buffet dot))
      :load (fn [self id] (:load buffet id))
      :retrieve (fn [self &opt what opts]
                  (default what :all)
                  (default opts @{:id? true})
                  (:retrieve buffet what opts))})
  (def i (rpc/server functions (self :host) (self :current-port)))
  (put-in self [:servers name] @{:buffet buffet
                                 :instance i
                                 :port (self :current-port)})
  (:_inc-port self))

(defn- start-operator [self]
  (def fns
    {:call (fn [self]
             (-> {:host (self :host)
                  :port (self :host)
                  :servers (map |({:port ($ :port)}) (self :servers))}))})
  (put self :operator (rpc/server fns (self :host) (self :current-port)))
  (:_inc-port self))

(defn- run [self]
  (:_start-operator self)
  (each s (self :buffets)
    (:_add-server self s))
  self)

(defn- visit [self name visitor]
  (def server (get-in self [:servers name]))
  (assert server "Your server is not running")
  (def v (rpc/client (self :host) (server :port) visitor))
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
    :servers @{}
    :visitors @[]
    :host "localhost"
    :current-port 9000
    :operator nil
    :run run
    :visit visit
    :close close
    :_add-server add-server
    :_inc-port inc-port
    :_start-operator start-operator})

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
