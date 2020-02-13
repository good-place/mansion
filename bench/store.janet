(import tahani :as t)
(import ../mansion/store :as ms)

(def db-name "peoplebench")

(def recs 100000)

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
    (def ids (:retrieve s {:job "Programmer"}))
    (printf "Retrieve %i ids in %f" (length (ids 0)) (wall-time))
    (start-clock)
    (def rsi (:retrieve s {:job "Programmer"} {:populate? :iter}))
    (printf "Populate %i records from iterator in %f" (length (rsi 0)) (wall-time))
    (start-clock)
    (def rsm (seq [id :in (ids 0)] (:load s id)))
    (printf "Manualy retrieve %i records in %f" (length rsm) (wall-time))
))
