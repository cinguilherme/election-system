(ns hooks.stateflow
  (:require [clj-kondo.hooks-api :as api]))

(defn- transform-steps [steps]
  (if (empty? steps)
    [(api/token-node nil)]
    (let [[step & more] steps]
      (if (and (api/vector-node? step)
               (pos? (count (:children step)))
               (even? (count (:children step))))
        (let [pairs (partition 2 (:children step))
              lhss (map first pairs)]
          [(api/list-node
            (list (api/token-node 'let)
                  step
                  (api/list-node
                   (list* (api/token-node 'list)
                          (concat lhss (transform-steps more))))))])
        (cons step (transform-steps more))))))

(defn flow [{:keys [node]}]
  (let [[_ desc & steps] (:children node)]
    {:node (api/list-node
            (list* (api/token-node 'list)
                   desc
                   (transform-steps steps)))}))
