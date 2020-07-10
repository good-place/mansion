(import jhydro :as j)

(defn hash2hex [data ctx hash-count] (freeze (j/util/bin2hex (j/hash/hash hash-count data ctx))))

(defn union [sets]
  (def head (first sets))
  (while (not= 1 (length sets))
    (let [aset (array/pop sets)]
      (each i aset
        (when (not (find-index |(= i $) head)) (array/push head i)))))
  (first sets))

(defn intersect [sets]
  (while (not= 1 (length sets))
    (let [head (first sets)
          aset (array/pop sets)]
      (put sets 0 (filter (fn [i] (find-index |(deep= i $) aset)) head))))
  (first sets))
