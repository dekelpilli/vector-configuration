(ns vector-configurator.core)

(defrecord VectorComponent [kind inputs config])

(def ^:private kind-order {:source 0 :transform 1 :sink 2})

(defn- throw-no-component! [c name]
  (throw (ex-info (str "Could not find component '" name "' in config") c)))

(defn- throw-wrong-kind! [component]
  (throw (ex-info "Component kind incompatible with operation" component)))

(defn- put-component
  ([c name kind inputs config] (->> (->VectorComponent kind inputs config)
                                    (put-component c name)))
  ([c name component] (update c ::components assoc name component)))

(defn- force-get-component [c name]
  (if-let [component (get-in c [::components name])]
    component
    (throw-no-component! c name)))

(defn- update-existing-component [c name f]
  (update c ::components update name
          (fn [component]
            (if component
              (f component)
              (throw-no-component! c name)))))

(defn- replace-consumed-name [component old-name new-name]
  (update component
          :inputs
          #(cond-> %
                   (% old-name) (-> (disj old-name)
                                    (conj new-name)))))

(defn ->config [c]
  (reduce-kv
    (fn [vector-config component-name component]
      (let [section (-> (:kind component)
                        name
                        (str \s)
                        keyword)]
        (->> (assoc (:config component) :inputs (:inputs component))
             (update vector-config section assoc component-name))))
    (assoc (::global c)
      :sources {}
      :transforms {}
      :sinks {})
    (::components c)))

(defn begin-config
  ([] (begin-config {}))
  ([global] {::global     global
             ::components {}}))

(defn add-component [c kind name inputs config]
  (if (get-in c [::components name])
    (throw (ex-info (str "Component '" name "' already exists") c))
    (put-component c kind name (set inputs) config)))

(defn add-source [c name config] (add-component c name :source nil config))
(defn add-sink [c name inputs config] (add-component c name :sink inputs config))

(defn add-transform [c name inputs config] (add-component c name :transform inputs config))

(defn inject-transform-before [c downstream-name name config]
  (let [downstream-component (force-get-component c downstream-name)]
    (when (= :source (:kind downstream-component))
      (throw-wrong-kind! downstream-component))
    (-> (add-component c name :transform (:inputs downstream-component) config)
        (put-component downstream-name (assoc downstream-component :inputs #{name})))))

(defn inject-transform-after [c upstream-name name config]
  (let [upstream-component (force-get-component c upstream-name)
        _ (when (= :sink (:kind upstream-component))
            (throw-wrong-kind! upstream-component))
        c (add-component c name :transform #{upstream-name} config)]
    (update
      c ::components
      #(update-vals
         %
         (fn [component]
           (replace-consumed-name component upstream-name name))))))

(defn link [c name other-name]
  (let [[[_upstream-component upstream-name]
         [downstream-component downstream-name]] (sort-by
                                                   (comp kind-order :kind first)
                                                   [[(force-get-component c name) name]
                                                    [(force-get-component c other-name) other-name]])]
    (->> (update downstream-component :inputs conj upstream-name)
         (put-component c downstream-name))))

(defn unlink [c name other-name]
  (-> (update-existing-component c name (fn [component] (update component :inputs disj other-name)))
      (update-existing-component other-name (fn [component] (update component :inputs disj name)))))

(defn remove-component [c name]
  (update c ::components
          (fn [components]
            (-> (dissoc components name)
                (update-vals #(update % :inputs disj name))))))

(comment
  ;          root----------------------------\
  ;         /    \        \         \         \
  ;     sink1    sink2   sink3       \        |
  ;      |   \  /   |       \         \       |
  ;      x1   x2   x3---\    \         \      x5
  ;        \__|___/     |     \         \
  ;           |         |      |         |
  ;           x4        |      |         |
  ;           |         |      |         |
  ;         source1  source2  source3   source4

  (-> (begin-config)
      (add-sink "sink1" ["x1" "x2"] {})
      (add-sink "sink2" ["x2" "x3"] {})
      (add-sink "sink3" ["source3"] {})
      (add-source "source1" {})
      (add-source "source2" {})
      (add-source "source3" {})
      (add-source "source4" {})
      (add-transform "x1" ["x4"] {})
      (add-transform "x2" ["x4"] {})
      (add-transform "x3" ["x4" "source3"] {})
      (add-transform "x4" ["source1" "source2"] {})
      (add-transform "x5" nil {})
      (remove-component "x5")
      ->config))
