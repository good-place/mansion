(import test/helper :prefix "" :exit true)

(import tahani :as t)

(import ../mansion/reception :as mr)
(import ../mansion/store :as ms)
(import ../mansion/utils :as mu)

(start-suite 1)

(def names ["peopletest" "commentstest"])

(defer (each name names (t/manage/destroy name))
  (each name names (:close (ms/create name @{:to-index ["name"]})))
  (def r (mr/open names))
  (assert r "Reception cannot be opened")
  (assert-no-error "Reception cannot be ran" (:run r))
  (def visitor (:visit r "peopletest" "pp"))
  (assert visitor "Visitor was not created")
  (assert (= 48 (:load visitor "counter")) "Counter is not zero")
  (def i (:save visitor {"name" "pepe"}))
  (assert (= i "1") "Bad id generated")
  (assert (deep= (:load visitor i) {"name" "pepe"}) "Bad record is loaded")
  (assert (deep= (:retrieve visitor) @[@[@["1" {"name" "pepe"}]]]) "Bad records are retrieved")
  (:save visitor {"name" "vasa"})
  (assert (deep= (:retrieve visitor) @[@[@["2" {"name" "vasa"}] @["1"{"name" "pepe"}]]]) "Bad records are retrieved")
  (:close r))

(end-suite)
