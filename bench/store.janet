(import tahani :as t)
(import ../mansion/store :as ms)

(def db-name "peoplebench")

(def recs 10000)

(defer (t/manage/destroy db-name)
  (with [s (ms/create db-name @{:to-index [:name :job :pet]})]
    (var n (os/clock))
    (defn start-clock [] (set n (os/clock)))
    (defn wall-time [] (- (os/clock) n))
    (printf "Start with %i records" recs)
    (with [batch (t/batch/create) |(:write s $)]
      (for i 0 recs (:save s {:name (string "Joker-" i) :job (if (odd? i) "Programmer" "Gardener")} batch)))
    (printf "Save records in %f" (- (os/clock) n))
    (start-clock)
    (def ids (:find-by s :job "Programmer"))
    (printf "Retrieve %i ids in %f" (length ids) (wall-time))
    (start-clock)
    (def rsi (:find-by s :job "Programmer" :iter))
    (printf "Populate %i records from iterator in %f" (length rsi) (wall-time))
    (start-clock)
    (def rsl (:find-by s :job "Programmer" :load))
    (printf "Populate %i records from store in %f" (length rsl) (wall-time))
    (start-clock)
    (def rsm (seq [id :in ids] (:load s id)))
    (printf "Manualy retrieve %i records in %f" (length rsm) (wall-time))
))
