# @fixme: where are all the docs?
(import tahani :as t)
(import mansion/utils :as u)

(defn must-err [exp got]
  (string/format "must be %s got: %s" exp (string (type got))))

(defn- _make-index [self field data]
  (string field (u/hash2hex data (self :ctx) (self :hash-size))))

(defn- _create [self]
  (put self :db (t/open (self :name) :eie))
  (def batch (t/batch/create))
  (defer (:destroy batch)
    (:put batch "hash-size" (string (marshal (self :hash-size))))
    (:put batch "to-index" (string (marshal (self :to-index))))
    (:put batch "ctx" (self :ctx))
    (:put batch "counter" "0")
    (loop [i :in (self :to-index)]
      (:put batch
            (string i (string/repeat "0" (+ (* (self :hash-size) 2) 1)))
            "\0"))
    (:write self batch))
  self)

(defn- _open [self]
  (def db (t/open (self :name) :eim))
  (put self :db db)
  (def batch (t/batch/create))
  (defer (:destroy batch)
    (put self :hash-size (unmarshal (:get db "hash-size")))
    (put self :to-index (unmarshal (:get db "to-index")))
    (put self :ctx (:get db "ctx"))
    (:write self batch))
  self)

(defn- close [self]
  (:close (self :db)))

(defn- _get [self id]
  (assert (string? id) (must-err "string" id))
  (-?> (:get (self :db) id) (unmarshal)))

(defn- write [self batch]
  (assert (= (type batch) :tahani/batch) (must-err "tahani/batch" batch))
  (defer (:destroy batch) (:write batch (self :db))))

(defn- save [self dot &opt batch]
  (var own-batch? (not batch))
  (def [id data]
    (if (tuple? dot)
      dot
      (let [id (-> :db self (:get "counter") scan-number inc string)]
        (-> :db self (:put "counter" id)) [id dot])))
  (assert (string? id) (must-err "string" id))
  (assert (struct? data) (must-err "struct" data))
  (default batch (t/batch/create))
  (assert (= (type batch) :tahani/batch) (must-err "tahani/batch" batch))
  (let [md (freeze (marshal data))]
    (:put batch id md)
    (each f (self :to-index)
      (when-let [d (get data f)]
        (let [mf (:_make-index self f d)
              start (string mf "0")]
          (unless (:get (self :db) start) (:put batch start d))
          (:put batch (string mf id) "\0"))))
    (when own-batch?
      (:write self batch)
      (:destroy batch))
    id))

(defn- load [self id]
  (assert (string? id) (must-err "string" id))
  (:_get self id))

(defn- _by-field [self field term iter]
  (assert (or (string? field) (keyword? field)) (must-err "keyword or string" field))
  (assert (string? term) (must-err "string" term))
  (assert (find |(= $ field) (self :to-index)) "Can only search in indexed fields") # move to init
  (def ids @[])
  (let [mf (:_make-index self field term)
        start (string mf "0")
        id-start (+ (length field) (* 2 (self :hash-size)))]
    (:seek iter start)
    (while (:valid? iter)
      (:next iter)
      (if-let [k (:key iter)
               id (and (string/has-prefix? mf k) (string/slice k id-start))]
        (array/push ids id) (break)))
    ids))

(defn- _all [self iter &opt limit]
  (var l (dec (or limit math/inf)))
  (def counter (:get (self :db) "counter"))
  (def ids @[])
  (when (not (= counter "0"))
    (:seek iter counter)
    (array/push ids (:key iter))
    (:prev iter)
    (while (:valid? iter)
      (-= l 1)
      (let [k (:key iter)]
        (array/push ids k)
        (when (or (= k "1") (zero? l)) (break)))
      (:prev iter)))
  @[ids])

# @fixme Opt iterator
(defn- retrieve [self what &opt options]
  (default options @{})
  (when (options :id?) (put options :populate? true))
  (with [iter (t/iterator/create (self :db)) |(:destroy $)] #@fixme buffet prop
    (def ids
      (cond
        (= what :all) (:_all self iter (options :limit))
        (struct? what)
        (seq [[k v] :pairs what] (:_by-field self k v iter))
        (indexed? what) (do (put options :populate? true) what)
        (error "Unknow type of what")))
    (if (options :populate?)
      (map |(seq [id :in $]
              (:seek iter id)
              (def v (unmarshal (:value iter)))
              (if (options :id?) @[id v] v)) ids)
      ids)))

(def Buffet
  @{:name nil
    :to-index nil
    :ctx "-tahani-"
    :hash-size 16
    :_db nil
    :_make-index _make-index
    :_get _get
    :_create _create
    :_open _open
    :_by-field _by-field
    :_all _all
    :close close
    :write write
    :save save
    :load load
    :retrieve retrieve})

(defn create [name &opt buffet]
  (default buffet @{:to-index []})
  (assert (string? name) (must-err "string" name))
  (assert (table? buffet) (must-err "table" buffet))
  (assert (tuple? (buffet :to-index)) (must-err "tuple" (buffet :to-index)))
  (:_create
    (-> buffet
        (table/setproto Buffet)
        (put :name name))))

(defn open [name]
  (assert (string? name) (must-err "string" name))
  (:_open
    (-> @{} (table/setproto Buffet) (put :name name))))
