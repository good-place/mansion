(import tahani :as t)
(import mansion/utils :as u)

(defn- _make-index [self field data]
  (string field "-" (u/hash2hex data (self :ctx))))

(defn- _open [self]
  (put self :db (t/open (self :name))))

(defn- _assert-store [self]
  (unless (:_get self "counter")
          (def batch (t/batch/create))
          (:put batch "counter" "0")
          (loop [i :in (self :to-index)]
            (:put batch (string i "-0000000000000000-0") "\0"))
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
    # @fixme this is very naive
    # key field-hash-id?
    (loop [f :in (self :to-index)
           :let [d (get data f)
                 mf (:_make-index self f d)
                 index (array/push (or (:_get self mf) @[d]) id)]]
      (:put batch mf (freeze (marshal index))))
    (:_write self batch)
    (:destroy batch)
    id))

(defn- load [self id] (:_get self id))

(defn- find-by [self field term]
  (assert (find |(= $ field) (self :to-index)))
  (seq [id :in (array/slice (:_get self (:_make-index self field term)) 1 -1)] (:_get self id)))

(defn- find-all [self query]
  (seq [[k v] :pairs query] (:find-by self k v)))

(def Store
  @{:name nil
    :to-index nil
    :ctx "-tahani-"
    :_db nil
    :_make-index _make-index
    :_get _get
    :_write _write
    :_open _open
    :_assert-store _assert-store
    :close close
    :save save
    :load load
    :find-by find-by
    :find-all find-all})

(defn create [name &opt to-index]
  (default to-index [])
  (assert (and (tuple? to-index) (all |(keyword? $) to-index)))
  (def s (-> @{} (table/setproto Store) (merge-into {:name name :to-index to-index})))
  (:_open s)
  (:_assert-store s)
  s)
