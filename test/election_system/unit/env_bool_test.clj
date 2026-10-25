(ns election-system.unit.env-bool-test
  "env-bool must accept the forms the compose files actually use. `Boolean/parseBoolean`
   only understands \"true\", so `CHAOS_ENABLED=1` silently disabled the chaos runtime."
  (:require [clojure.test :refer [deftest is testing]]
            [election-system.main]))

(def ^:private parse-bool @#'election-system.main/parse-bool)

(deftest the-forms-people-write
  (testing "truthy"
    (is (true? (parse-bool "1" false)))
    (is (true? (parse-bool "true" false)))
    (is (true? (parse-bool "TRUE" false)))
    (is (true? (parse-bool "yes" false)))
    (is (true? (parse-bool "on" false))))
  (testing "falsy"
    (is (false? (parse-bool "0" true)))
    (is (false? (parse-bool "false" true)))
    (is (false? (parse-bool "" true))))
  (testing "missing uses the default"
    (is (true? (parse-bool nil true)))
    (is (false? (parse-bool nil false)))))
