(import test/helper :prefix "" :exit true)

(import tahani :as t)

(import ../mansion/reception :as mr)
(import ../mansion/store :as ms)
(import ../mansion/utils :as mu)

(start-suite 1)

# (def db-name "peopletest")
#
# (defer (t/manage/destroy db-name)
#   (:close (ms/create db-name @{:to-index [:name :job :pet]}))
#   (def r (mr/server db-name))
#   (pp r)
#   (:close r))

(end-suite)
