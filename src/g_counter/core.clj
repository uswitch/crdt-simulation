(ns g-counter.core
  (:require [clojure.set :refer (intersection)]
            [clojure.core.async :refer (go go-loop chan alts! close! timeout >! <! sliding-buffer)]
            [clojure.core.async.lab :refer (broadcast)]
            [clojure.tools.logging :as log]
            [clojure.pprint :refer (print-table)]))

(defn resolve-conflict
  "Resolves conflict between counters."
  [& counters]
  (apply merge-with max counters))

(defn inc-id
  [counter id]
  (update-in counter [id] (fnil inc 0)))

(comment
  (resolve-conflict {:a 12 :b 14 :c 100} {:a 4 :b 42 :d 1})
  (resolve-conflict {:a 12 :b 14 :c 100})
  (resolve-conflict)
  (resolve-conflict {:a 12 :b 14 :c 100} {:a 4 :b 42 :d 1} {:d 2}))

;;;

(defprotocol Peer
  (in    [this] "Returns in-channel, accepts counters.")
  (out   [this] "Returns out-channel, spews counters.")
  (stop! [this] "Stops the peer"))

;; could be sampled instead
(def INTERVAL 10)

(defn create-state-peer!
  "Creates and returns a peer. The peer will run in a go-thread."
  [shared-state id]
  (let [control-ch (chan)
        in         (chan (sliding-buffer 1))
        out        (chan (sliding-buffer 1))]
    (log/info "Starting" id)
    (go-loop
     [timeout-ch (timeout INTERVAL)]
     (log/debug "current counter for" id ":" (get @shared-state id))
     (let [[val ch] (alts! [control-ch in timeout-ch])]
       (condp = ch

         control-ch (log/info "Stopping" id)

         in (do (log/debug "got counter, updating")
                (swap! shared-state
                       (fn [state new-counter]
                         (update-in
                          state [id]
                          (partial resolve-conflict new-counter)))
                       val)
                (recur timeout-ch))

         timeout-ch (do (log/debug "timeout fired, incrementing for" id)
                        (let [new-shared-state (swap! shared-state
                                                      (fn [state]
                                                        (update-in state [id]
                                                                   #(inc-id % id))))]
                          (>! out (get new-shared-state id)))
                        (recur (timeout INTERVAL)))

         ; default
         (throw (Exception. "Unhandled case - programming error. You should probably use alt! instead of alts!")))))
    (reify Peer
      (in    [this] in)
      (out   [this] out)
      (stop! [this] (close! control-ch)))))

(defn- uuid
  []
  (.toString (java.util.UUID/randomUUID)))

(defn create-op-peer!
  "Creates and returns a op peer. The peer will run in a go-thread."
  [shared-state id]
  (let [control-ch (chan)
        in         (chan 1024)
        out        (chan 1024)]
    (log/info "Starting" id)
    (go-loop
     [success-messages #{}
      timeout-ch (timeout INTERVAL)]
     (log/debug "current counter for" id ":" (get @shared-state id))
     (let [[msg ch] (alts! [control-ch in timeout-ch])]
       (condp = ch

         control-ch (log/info "Stopping" id)

         in (do (log/info "got counter, updating")
                (if (and (contains? success-messages msg)
                         true ;; pre-condition
                         )
                  (recur success-messages timeout-ch)
                  (do
                    (swap! shared-state update-in [id] (fnil inc 0))
                    (recur (conj success-messages msg) timeout-ch))))

         timeout-ch (do (log/debug "timeout fired, incrementing for" id)
                        (swap! shared-state update-in [:true-count] (fnil inc 0))
                        (>! out (uuid))
                        (recur success-messages (timeout INTERVAL)))

         ; default
         (throw (Exception. "Unhandled case - programming error. You should probably use alt! instead of alts!")))))
    (reify Peer
      (in    [this] in)
      (out   [this] out)
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

(defn op-based-network
  "Starts a broadcaster matching the assumptions for the op-based
  CRDTs. That is:

  - Every update eventually reaches the causal history of every replica
  - The delivery order <d respects downstream preconditions in
    downstream functions.

  max-lag is the maximum number of ms a message may be delayed in the
  network."
  [max-lag peers]
  (let [ins (map in peers)
        outs (map out peers)
        control-ch (chan)]
    (go-loop
     [msgs #{}
      t (timeout 10)]
     (let [[msg ch] (alts! (cons t (cons control-ch outs)))]
       (cond

        (= ch t) (do
                   (doseq [msg msgs]
                     (doseq [in ins]
                       (go
                        (<! (timeout (long (* (rand) max-lag))))
                        (>! in msg))))
                   (recur msgs (timeout 10)))

        (= ch control-ch) (log/info "Terminating op based network")

        msg (do
             (log/info "received" msg)
             (log/info "broadcasting" msg "infinitly often")
             (recur (conj msgs msg) t))

        :else (recur msgs t))))
    (reify Broadcaster
      (stop-broadcast! [this] (close! control-ch)))))

;;;

(defn state-print-peers
  [shared-state-snapshot]
  (print-table
   (let [platonic-counter (apply resolve-conflict (vals shared-state-snapshot))
         platonic-count (apply + (vals platonic-counter))]
     (concat
      (for [id (sort (keys shared-state-snapshot))
            :let [count (apply + (vals (get shared-state-snapshot id)))]]
        {"i" id
         "count" count
         "off" (- platonic-count count)
         "self" (get-in shared-state-snapshot [id id])})
      [{"i" "-", "count" platonic-count, "off" "-", "self" "-"}]))))

(defn op-print-peers
  [shared-state-snapshot]
  (print-table
   (let [platonic-count (:true-count shared-state-snapshot)]
     (concat
      (for [id (sort (keys (dissoc shared-state-snapshot :true-count)))
            :let [count (get shared-state-snapshot id)]]
        {"i" id
         "count" count
         "off" (- platonic-count count)
         "self" "-"})
      [{"i" "-", "count" platonic-count, "off" "-", "self" "-"}]))))

;;;

(defn make-system
  [broadcaster-fn n create-peer! print-peers]
  (let [shared-state (atom {})
        peers (doall (map (partial create-peer! shared-state) (range n)))
        broadcaster (broadcaster-fn peers)]
    (doseq [iter (range 3)]
      (print-peers @shared-state)
      (Thread/sleep 1500))
    (doall (map stop! peers))
    (stop-broadcast! broadcaster)
    (print-peers @shared-state)))

(defn make-graph-system
  [broadcaster-fn n create-peer!]
  (let [shared-state (atom {})
        peers (doall (map (partial create-peer! shared-state) (range n)))
        broadcaster (broadcaster-fn peers)]
    (Thread/sleep 2500)
    (let [shared-state-snapshot @shared-state
          counters (vals (dissoc shared-state-snapshot :true-count))
          drifts (map (partial - (:true-count shared-state-snapshot)) counters)]
      (log/info "drifts" drifts)
      (doall (map stop! peers))
      (stop-broadcast! broadcaster)
      drifts)))

(defn- avg
  [coll]
  (float (/ (apply + coll) (count coll))))

(defn- medium
  [coll]
  (nth (sort coll) (int (/ (count coll) 2))))

(defn op-experiment
  []
  (doseq [max-lag [16 32 64]]
    (doseq [sample (range 3)]
      (let [drifts (make-graph-system (partial op-based-network max-lag)
                                      5
                                      create-op-peer!)]
        (println
         max-lag
         sample
         (apply min drifts)
         (avg drifts)
         (medium drifts)
         (apply max drifts))))))

(comment

  (op-experiment)

  (def p (create-peer! 42))
  (stop! p)
  (clojure.core.async/put! (in p) {3 10})

  (make-system (partial lossy-broadcaster 0.5 0.5) 3)
  (make-system (partial lossy-broadcaster 0.9 0.1) 3)
  (make-system (partial lossy-broadcaster 0.1 0.1) 8)
  (make-system perfect-broadcaster 3)

  (make-system (partial lossy-broadcaster 0.5 0.5) 5 create-state-peer! state-print-peers)
  (make-system (partial lossy-broadcaster 0.75 0.75) 10)
  (make-system perfect-broadcaster 10)


  (make-system (partial op-based-network 1000) 5)
  (make-system (partial op-based-network 100) 5)

  (make-system (partial op-based-network 1000) 5 create-op-peer! op-print-peers)
  )
