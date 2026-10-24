(ns {{ns}}.core-test
  (:require [clojure.test :refer [deftest is]]
            [{{ns}}.core :as core]))

(deftest the-module-loads
  (is (= "hello, world" (core/hello "world"))))
