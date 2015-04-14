(ns g-counter.lens
  (:require
    [g-counter.functor :as functor]))

; Lenses
(defn in [k f]
  (fn [m]
    (-> m
        (get k)                            ; Deconstruct
        f                                  ; Apply function
        (functor/fmap #(assoc m k %)))))   ; Apply reconstruction

(defn adjacent
  [[x y]]
  (functor/->Sequence
    [[x (inc y)] [(inc x) y]
     [(dec x) y] [x (dec y)]]))

(defn minutes [f]
  (fn [seconds]
    (-> seconds
        (/ 60)                             ; Deconstruct
        f                                  ; Apply function
        (functor/fmap (partial * 60)))))   ; Apply reconstruction

; Lens operations
(defn update [x lens f]
  (-> x
      ((lens (comp functor/->Identity f)))
      (get :value)))

(defn put [x lens value]
  (update x lens (constantly value)))

(defn view [x lens]
  (-> x
      ((lens functor/->Constant))
      (get :value)))

(comment
  (fact "The In Lens supports the Lens operations."
        (-> {:x 1 :y 2} (update (partial in :x) inc)) => {:x 2 :y 2}
        (-> {:x 1 :y 2} (put (partial in :x) 99)) => {:x 99 :y 2}
        (-> {:x 1 :y 2} (view (partial in :x))) => 1)

  (fact "The Minutes Lens supports the Lens operations."
        (-> 120 (update minutes dec)) => 60
        (-> 120 (put minutes 4)) => 240
        (-> 120 (view minutes)) => 2)


  (fact "Lenses compose as functions."
        (-> {:time 1}
            (update (comp (partial in :time) minutes) inc))
        => {:time 61}))
