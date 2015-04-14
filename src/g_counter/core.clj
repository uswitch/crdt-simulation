(ns g-counter.core
  (:require [clojure.set :refer (intersection)]
            [clojure.core.async :refer (go-loop chan alts! close! timeout >! sliding-buffer)]
            [clojure.core.async.lab :refer (broadcast)]
            [clojure.tools.logging :as log]
            [clojure.pprint :refer (print-table)]))

(defn resolve-conflict
  "Resolves conflict between counters local and remote."
  [& counters]
  (reduce (fn [c1 c2]
            (merge c1 c2
                   (into
                    {}
                    (for [k (intersection (set (keys c1)) (set (keys c2)))]
                      [k (max (get c1 k) (get c2 k))]))))
          nil
          counters))

(defn inc-id
  [counter id]
  (update-in counter [id] (fnil inc 0)))

(resolve-conflict {:a 12 :b 14 :c 100} {:a 4 :b 42 :d 1})
(resolve-conflict {:a 12 :b 14 :c 100})
(resolve-conflict)
(resolve-conflict {:a 12 :b 14 :c 100} {:a 4 :b 42 :d 1} {:d 2})

;;;

(defprotocol Peer
  (in [this] "Returns in-channel, accepts counters.")
  (out [this] "Returns out-channel, spews counters.")
  (stop! [this] "Stops the peer"))

;; could be sampled instead
(def TIMEOUT 100)

(defn create-peer!
  "Creates and returns a peer. The peer will run in a go-thread."
  [shared-state id]
  (let [control-ch (chan)
        in (chan (sliding-buffer 1))
        out (chan (sliding-buffer 1))]
    (log/info "Starting" id)
    (go-loop
     [timeout-ch (timeout TIMEOUT)]
     (log/debug "current counter for" id ":" (get @shared-state id))
     (let [[val ch] (alts! [control-ch in timeout-ch])]
       (condp = ch

         control-ch (log/info "Stopping" id)

         in (do (log/debug "got counter, updating")
                ;; ye-gods! should use lenses
                ;; see https://github.com/ctford/journey-through-the-looking-glass
                (swap! shared-state
                       (fn [state new-counter] (update-in state [id]
                                                         (partial
                                                          resolve-conflict
                                                          new-counter)))
                       val)
                (recur timeout-ch))

         timeout-ch (do (log/debug "timeout fired, incrementing for" id)
                        (let [new-shared-state (swap! shared-state
                                                      (fn [state]
                                                        (update-in state [id]
                                                                   #(inc-id % id))))]
                          (>! out (get new-shared-state id)))
                        (recur (timeout TIMEOUT)))

         ; default
         (throw (Exception. "Unhandled case - programming error. You should probably use alt! instead of alts!")))))
    (reify Peer
      (in [this] in)
      (out [this] out)
      (stop! [this] (close! control-ch)))))

;;;

(defprotocol Broadcaster
  (stop-broadcast! [this] "Terminates the broadcaster"))

(defn perfect-broadcaster
  "Starts a perfect broadcaster which forwards anything from every out
  to every in."
  [peers]
  (let [all-in (apply broadcast (map in peers))
        outs (map out peers)
        control-ch (chan)]
    (go-loop
     []
     (let [[val ch] (alts! (cons control-ch outs))]
       (when-not (= ch control-ch)
         (log/info "broadcasting" val)
         (>! all-in val)
         (recur))))
    (reify Broadcaster
      (stop-broadcast! [this] (close! control-ch)))))

(defn lossy-broadcaster
  "Starts a lossy broadcaster which forwards things with probability p
  from every out to every in, each with probability q."
  [p q peers]
  (let [ins (map in peers)
        outs (map out peers)
        control-ch (chan)]
    (go-loop
     []
     (let [[val ch] (alts! (cons control-ch outs))]
       (when-not (= ch control-ch)
         (log/info "received" val)
         (when (< (rand) p)
           (log/info "broadcasting" val)
           (doseq [in ins]
             (when (< (rand) q)
               (>! in val))))
         (recur))))
    (reify Broadcaster
      (stop-broadcast! [this] (close! control-ch)))))

;;;

(defn print-peers
  [shared-state-snapshot]
  (print-table
   (let [platonic-counter (apply resolve-conflict (vals shared-state-snapshot))
         platonic-count (apply + (vals platonic-counter))]
     (concat
      (for [id (sort (keys shared-state-snapshot))
            :let [count (apply + (vals (get shared-state-snapshot id)))]]
        {"i" id
         "count" count
         "-" (- platonic-count count)
         "own" (get-in shared-state-snapshot [id id])})
      [{"i" "-", "count" platonic-count, "-" "-", "own" "-"}]))))

;;;

(defn make-system
  [broadcaster-fn n]
  (let [shared-state (atom {})
        peers (doall (map (partial create-peer! shared-state) (range n)))
        broadcaster (broadcaster-fn peers)]
    (doseq [iter (range 3)]
      (print-peers @shared-state)
      (Thread/sleep 1500))
    (stop-broadcast! broadcaster)
    (doall (map stop! peers))
    (print-peers @shared-state)))

(comment
  (def p (create-peer! 42))
  (stop! p)
  (clojure.core.async/put! (in p) {3 10})

  (make-system (partial lossy-broadcaster 0.5 0.5) 3)
  (make-system (partial lossy-broadcaster 0.9 0.1) 3)
  (make-system (partial lossy-broadcaster 0.9 0.9) 3)
  (make-system perfect-broadcaster 3)

  (make-system (partial lossy-broadcaster 0.5 0.5) 10)
  (make-system (partial lossy-broadcaster 0.75 0.75) 10)
  (make-system perfect-broadcaster 10)

  )
