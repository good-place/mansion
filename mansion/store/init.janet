(import tahani :as t)
(import mansion/utils :as u)

(defn- _make-index [self field data]
  (string field (u/hash2hex data (self :ctx) (self :hash-count))))

(defn- _open [self]
  (put self :db (t/open (self :name))))

(defn- _assert-store [self]
  (unless (:_get self "counter")
          (def batch (t/batch/create))
          (:put batch "counter" "0")
          (:_write self batch)
          (:destroy batch)))

(defn close [self]
  (:close (self :db)))

(defn- _get [self id]
   (-?> (:get (self :db) id) (unmarshal)))

(defn- _write [self batch]
   (:write batch (self :db)))

(defn- save [self data]
  (let [md (freeze (marshal data))
        id (-> (self :db) (:get "counter") (scan-number) (inc) (string))
        batch (t/batch/create)]
    (:put batch "counter" id)
    (:put batch id md)
    (each f (self :to-index)
      (if-let [d (get data f)]
        (let [mf (:_make-index self f d)
              start (string mf "-0")]
          (unless (:get (self :db) start) (:put batch start "\0"))
          (:put batch (string mf "-" id) "\0"))))
    (:_write self batch)
    (:destroy batch)
    id))

(defn- load [self id] (:_get self id))

(defn- find-by [self field term &opt populate?]
  (assert (find |(= $ field) (self :to-index)))
  (default populate? false)
  (let [ids @[]
        mf (:_make-index self field term)
        start (string mf "-0")
        iter (t/iterator/create (self :db))]
    (:seek iter start)
    (when (:valid? iter)
      (while (:valid? iter)
        (:next iter)
        (def k (:key iter))
        (if (string/has-prefix? mf (:key iter))
          (array/push ids (last (string/split "-" k)))
          (break)))
      (case populate?
        :iter
        (seq [id :in ids]
             (:seek iter id)
             (unmarshal (:value iter)))
        :load
        (seq [id :in ids] (:load self id))
        ids))))

(def Store
  @{:name nil
    :to-index nil
    :ctx "-tahani-"
    :hash-count 16
    :_db nil
    :_make-index _make-index
    :_get _get
    :_write _write
    :_open _open
    :_assert-store _assert-store
    :close close
    :save save
    :load load
    :find-by find-by})

(defn create [name &opt to-index]
  (default to-index [])
  (assert (and (tuple? to-index) (all |(keyword? $) to-index)))
  (def s (-> @{} (table/setproto Store) (merge-into {:name name :to-index to-index})))
  (:_open s)
  (:_assert-store s)
  s)
