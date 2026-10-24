(ns release
  "Release helper for the d-core module family.

   Run from the workspace root (the directory holding d-core-*):

     clojure -Sdeps '{:paths [\"election-system/scripts\"]}' -M -m release --check
     clojure -Sdeps '{:paths [\"election-system/scripts\"]}' -M -m release --bump patch
     clojure -Sdeps '{:paths [\"election-system/scripts\"]}' -M -m release --bump minor --apply

   `--check` validates every module's `.version` and prints the matrix (exit 1 on a
   bad version). `--bump` prints the plan; `--apply` writes the files; adding `--tag`
   creates `vX.Y.Z` in each module's git repo. Nothing is committed."
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str])
  (:gen-class))

(defn modules
  "The d-core-* module directories under `root`, newest name last."
  [root]
  (->> (.listFiles (io/file root))
       (filter #(.isDirectory ^java.io.File %))
       (filter #(str/starts-with? (.getName ^java.io.File %) "d-core-"))
       (filter #(.isDirectory (io/file % "src")))
       (sort-by #(.getName ^java.io.File %))))

(defn version-of
  [m]
  (let [f (io/file m ".version")]
    (when (.exists f) (str/trim (slurp f)))))

(defn parse-version
  [v]
  (when-let [[_ major minor patch] (and v (re-matches #"(\d+)\.(\d+)\.(\d+)" v))]
    {:major (Long/parseLong major) :minor (Long/parseLong minor) :patch (Long/parseLong patch)}))

(defn bump
  [v kind]
  (let [{:keys [major minor patch]} (parse-version v)]
    (case kind
      :major (str (inc major) ".0.0")
      :minor (str major "." (inc minor) ".0")
      :patch (str major "." minor "." (inc patch)))))

(defn dirty?
  [m]
  (let [{:keys [out]} (sh/sh "git" "-C" (str m) "status" "--porcelain")]
    (not (str/blank? out))))

(defn rows
  [root]
  (for [m (modules root)
        :let [v (version-of m)]]
    {:module (.getName ^java.io.File m)
     :dir m
     :version v
     :valid? (boolean (parse-version v))
     :dirty? (dirty? m)}))

(defn print-table
  [rs]
  (doseq [{:keys [module version valid? dirty?]} rs]
    (println (format "%-28s %-8s %-4s %s"
                     module
                     (or version "-")
                     (if valid? "ok" "BAD")
                     (if dirty? "dirty" "")))))

(defn check!
  [rs]
  (print-table rs)
  (let [bad (remove :valid? rs)]
    (when (seq bad)
      (println "\ninvalid or missing .version in:" (mapv :module bad))
      (System/exit 1))
    (println "\n" (count rs) "module(s) checked")
    (when-let [dirty (seq (filter :dirty? rs))]
      (println "warning: uncommitted changes in" (mapv :module dirty)))))

(defn plan
  [rs kind]
  (doseq [{:keys [module version]} rs]
    (println (format "%-28s %-8s -> %s" module version (bump version kind))))
  (println "\n" (count rs) "module(s); re-run with --apply to write the files"))

(defn apply!
  [_root rs kind]
  (doseq [{:keys [dir module version]} rs]
    (spit (io/file dir ".version") (str (bump version kind) "\n"))
    (println "wrote" (str module "/.version") "->" (bump version kind))))

(defn tag!
  [_root rs kind]
  (doseq [{:keys [dir module version]} rs]
    (let [tag (str "v" (bump version kind))
          dir (str dir)
          exists? (zero? (:exit (sh/sh "git" "-C" dir "rev-parse" "-q" "--verify"
                                       (str "refs/tags/" tag))))]
      (if exists?
        (println "tag exists:" module tag)
        (let [{:keys [exit err]} (sh/sh "git" "-C" dir "tag" "-a" tag "-m" tag)]
          (println (if (zero? exit) (str "tagged " module " " tag) (str "FAILED " module ": " err))))))))

(defn parse-args
  [args]
  (loop [args (seq args) opts {:mode :plan}]
    (if-let [a (first args)]
      (case a
        "--check" (recur (next args) (assoc opts :mode :check))
        "--root" (recur (nnext args) (assoc opts :root (second args)))
        "--bump" (recur (nnext args) (assoc opts :kind (keyword (second args))))
        "--apply" (recur (next args) (assoc opts :apply? true))
        "--tag" (recur (next args) (assoc opts :tag? true))
        (recur (next args) opts))
      opts)))

(defn -main
  [& args]
  (let [{:keys [mode kind apply? tag? root]} (parse-args args)
        root (or root ".")
        rs (vec (rows root))]
    (case mode
      :check (check! rs)
      :plan (do (print-table rs)
                (if kind
                  (do (println) (plan rs kind)
                      (when apply? (apply! root rs kind))
                      (when tag? (tag! root rs kind)))
                  (println "\nusage: --check | --bump patch|minor|major [--apply] [--tag]"))))))
