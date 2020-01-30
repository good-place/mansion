(declare-project
  :name "mansion"
  :description "Place for tahani"
  :dependencies ["https://github.com/janet-lang/jhydro.git"
                 "https://github.com/pepe/tahani"])

(declare-source
 :name "mansion"
 :source @["mansion/"])

# run repl with tahani included
(phony "repl" ["build"] (os/execute ["janet" "-r" "-e" "(import mansion :as m)"] :p))
