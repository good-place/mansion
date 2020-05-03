(import test/helper :prefix "" :exit true)

(import tahani :as t)

(import ../mansion/reception :as mr)
(import ../mansion/buffet :as mb)
(import ../mansion/utils :as mu)

(start-suite 1)

(def names ["peopletest" "commentstest"])

(defer (each name names (t/manage/destroy name))
  (each name names (:close (mb/create name @{:to-index ["name"]})))
  (def r (mr/open names))
  (assert r "Reception cannot be opened")
  (assert-no-error "Reception cannot be ran" (:run r))
  (def v (:visit r "peopletest" "pp"))
  (assert v "Visitor was not created")
  (assert (= 48 (:load v "counter")) "Counter is not zero")
  (def i (:save v {"name" "pepe"}))
  (assert (= i "1") "Bad id generated")
  (assert (deep= (:load v i) {"name" "pepe"}) "Bad record is loaded")
  (assert (deep= (:retrieve v) @[@[@["1" {"name" "pepe"}]]]) "Bad records are retrieved")
  (:save v {"name" "vasa"})
  (assert (deep= (:retrieve v) @[@[@["2" {"name" "vasa"}] @["1"{"name" "pepe"}]]]) "Bad records are retrieved")
  (:close r))

(end-suite)
