;; https://github.com/ctford/journey-through-the-looking-glass/blob/master/src/journey_through_the_looking_glass/functor.clj

(ns g-counter.functor)

; Functors
(defn fsequence [f] (partial map f))

(defn fidentity [f] f)

(defn fconstant [_] identity)

(defn fin [k f] (fn [x] (update-in x [k] f)))

; Functor polymorphism
(defprotocol Functor
  (fmap [this f]))

(defrecord FunctorObject [functor value]
  Functor
  (fmap
    [this f]
    (-> this
        (get :value)                           ; Deconstruct
        ((functor f))                          ; Apply function
        ((partial ->FunctorObject functor))))) ; Reconstruct

(def ->Sequence (partial ->FunctorObject fsequence))
(def ->Identity (partial ->FunctorObject fidentity))
(def ->Constant (partial ->FunctorObject fconstant))
