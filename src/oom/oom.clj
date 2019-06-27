(ns oom.oom
  (:import (java.util.concurrent Executors TimeUnit))
  (:require [clojure.string :as str]))


(defn bytes-to-mb [nbytes]
  (/ nbytes (* 1024.0 1024.0)))


(defn show-mem []
  (println (format "memory (MBytes) %.1f max %.1f total %.1f free"
                   (bytes-to-mb (. (Runtime/getRuntime) maxMemory))
                   (bytes-to-mb (. (Runtime/getRuntime) totalMemory))
                   (bytes-to-mb (. (Runtime/getRuntime) freeMemory)))))


(defn take-1-sec [n]
  (when (zero? (mod n 100))
    (println (format "Task %d start" n)))
  (Thread/sleep 1000)
  (when (zero? (mod n 100))
    (println (format "Task %d end" n))))


(defn do-concurrently2
  [tasks c handler]
  (System/gc)
  (show-mem)
  (let [executor (Executors/newFixedThreadPool c)
        counter (atom 0)]
    ;; Submit tasks to run concurrently.
    (doseq [task tasks
            :let [handling-task (comp handler task)]]
      (-> executor (.execute handling-task))
      (swap! counter inc)
      (let [n @counter]
        (when (zero? (mod n (* 100 1000)))
          (println (format "Have enqueued %d tasks" n))
          (show-mem))))
    ;; shutdown executor once all tasks have been processed
    (-> executor .shutdown)
    (if true
      (-> executor (.awaitTermination Long/MAX_VALUE TimeUnit/DAYS))
      ;; else
      (loop []
        (let [qlen (.. executor getQueue size)]
          (println (format "executor qlen %d" qlen))
          (when (> qlen 0)
            (Thread/sleep 1000)
            (recur)))))))


(comment

(import '(java.util.concurrent Executors TimeUnit))

(load-file "src/oom/oom.clj")

(do

(require '[oom.oom :as o])
(System/gc)
(o/show-mem)


;;(def num-tasks 20)
;;(def num-tasks (* 1000 1000))
(def num-tasks (* 2 1000 1000))
;;(def num-tasks (* 1000 1000 1000))
(def tasks (map (fn [idx] (fn [] (o/take-1-sec idx)))
                (range num-tasks)))
;;(pprint tasks)
(def thread-pool-size 4)
(def handler identity)

)

(o/do-concurrently2 tasks thread-pool-size handler)


)
