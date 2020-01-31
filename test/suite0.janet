(import test/helper :prefix "" :exit true)

(import tahani :as t)

(import ../mansion/store :as ms)
(import ../mansion/utils :as mu)

(start-suite 0)

(def db-name "peopletest")

(defer (t/manage/destroy db-name)
  (with [s (ms/create db-name @{:to-index [:name :job :pet]})]
    (assert s "Store is not created")
    (def id (:save s {:name "Pepe" :job "Programmer" :pet "Cat"}))
    (assert id "Record is not saved")
    (assert (string? id) "Record id is not string")
    (assert (= id "1") "Id is not 1")
    (def r (:load s id))
    (assert r "Record is not loaded")
    (assert (struct? r) "Record is not struct")
    (assert (= (r :name) "Pepe") "Record has bad name")
    (with [batch (t/batch/create) |(:write s $)]
      (def id2 (:save s {:name "Jose" :job "Programmer" :pet "Cat"} batch))
      (def id3 (:save s {:name "Karl" :job "Gardener" :pet "Dog"} batch))
      (def id4 (:save s {:name "Pepe" :job "Gardener" :pet "Dog"} batch))
      (def id5 (:save s {:name "Joker" :job "Gardener" :pet "" :good-deeds []} batch)))
    (def rs (:find-by s :name "Pepe"))
    (assert (array? rs) "Records are not found by find-by")
    (assert (= (length rs) 2) "Not all records are found by find-by")
    (assert (deep= rs @["1" "4"]) "Not right ids found")
    (def rsi (:find-by s :name "Pepe" :iter))
    (assert (= (length rsi) 2) "Not all records are found by find-by with iterator population")
    (assert (deep= rsi @[{:name "Pepe" :job "Programmer" :pet "Cat"} {:name "Pepe" :job "Gardener" :pet "Dog"}]) "Not right records found")
    (def rsl (:find-by s :name "Pepe" :load))
    (assert (= (length rsl) 2) "Not all records are found by find-by with iterator population")
    (assert (deep= rsl @[{:name "Pepe" :job "Programmer" :pet "Cat"} {:name "Pepe" :job "Gardener" :pet "Dog"}]) "Not right records found"))
  (with [os (ms/open db-name)]
    (assert (:load os "1") "First record is not in opened db")
    (def rsi (:find-by os :name "Pepe" :iter))
    (assert (= (length rsi) 2) "Not all records are found by find-by with iterator population")
    (assert (deep= rsi @[{:name "Pepe" :job "Programmer" :pet "Cat"} {:name "Pepe" :job "Gardener" :pet "Dog"}]) "Not right records found")))

(end-suite)

