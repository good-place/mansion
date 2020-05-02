(import spork/rpc :as rpc)
(import mansion/store :as ms)

(defn server [db-name]
  (def store (ms/open db-name))
  (def functions
    @{:load (fn [self id] (:load store id))
      :retrieve (fn [self] (:retrieve store :all @{:id? true}))})
  (rpc/server functions "127.0.0.1" "9001"))
