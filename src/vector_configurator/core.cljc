(ns vector-configurator.core)

(defrecord VectorComponent [kind inputs config])

(def ^:private kind-order {:source 0 :transform 1 :sink 2})

(defn- throw-no-component! [c name]
  (throw (ex-info (str "Could not find component '" name "' in config") c)))

(defn- throw-wrong-kind! [component]
  (throw (ex-info "Component kind incompatible with operation" component)))

(defn- put-component
  ([c component-name kind inputs config] (->> (->VectorComponent kind (into #{} (map name) inputs) config)
                                              (put-component c component-name)))
  ([c component-name component] (update c ::components assoc (name component-name) component)))

(defn- try-get-component [c component-name]
  (get-in c [::components (name component-name)]))

(defn- force-get-component [c component-name]
  (if-let [component (try-get-component c component-name)]
    component
    (throw-no-component! c component-name)))

(defn- update-existing-component [c component-name f]
  (update c ::components update (name component-name)
          (fn [component]
            (if component
              (f component)
              (throw-no-component! c component-name)))))

(defn- replace-consumed-name [component old-name new-name]
  (update component
          :inputs
          #(cond-> %
                   (% old-name) (-> (disj (name old-name))
                                    (conj (name new-name))))))

(defn ->config [c]
  (reduce-kv
    (fn [vector-config component-name component]
      (let [section (-> (:kind component)
                        name
                        (str \s)
                        keyword)
            component-config (cond-> (:config component)
                                     (not= :sources section) (assoc :inputs (vec (:inputs component))))]
        (update vector-config section assoc component-name component-config)))
    (assoc (::global c)
      :sources {}
      :transforms {}
      :sinks {})
    (::components c)))

(defn begin-config
  ([] (begin-config {}))
  ([global] {::global     global
             ::components {}}))

(defn add-component [c kind component-name inputs config]
  (let [name (name component-name)]
    (if (get-in c [::components name])
      (throw (ex-info (str "Component '" name "' already exists") c))
      (put-component c kind name inputs config))))

(defn add-source [c name config] (add-component c name :source nil config))
(defn add-transform [c name inputs config] (add-component c name :transform inputs config))
(defn add-sink [c name inputs config] (add-component c name :sink inputs config))

(defn config-> [config]
  (let [global (dissoc config :sources :transforms :sinks)]
    (as-> (begin-config global) c
          (reduce-kv add-source c (:sources config))
          (reduce-kv (fn [c name component]
                       (add-transform c name (:inputs component) (dissoc component :inputs))) c (:transforms config))
          (reduce-kv (fn [c name component]
                       (add-sink c name (:inputs component) (dissoc component :inputs))) c (:sinks config)))))

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

(defn link
  "Links two components based on the order of their component type.
   If both are transforms, second-name will be downstream of first-name (i.e. first-name will be added as an input for second-name)."
  [c first-name second-name]
  (let [[[_upstream-component upstream-name]
         [downstream-component downstream-name]] (sort-by
                                                   (comp kind-order :kind first)
                                                   [[(force-get-component c first-name) first-name]
                                                    [(force-get-component c second-name) second-name]])]
    (->> (update downstream-component :inputs conj (name upstream-name))
         (put-component c downstream-name))))

(defn unlink [c name other-name]
  (-> (update-existing-component c name (fn [component] (update component :inputs disj other-name)))
      (update-existing-component other-name (fn [component] (update component :inputs disj name)))))

(defn- remove-from-components [components component-name]
  (-> (dissoc components component-name)
      (update-vals #(update % :inputs disj component-name))))

(defn remove-component [c component-name]
  (->> (name component-name)
       (update c ::components remove-from-components)))

(defn update-component [c component-name f]
  (update c ::components
          (fn [components]
            (let [component-name (name component-name)
                  component (get components component-name)]
              (if-let [result (f component)]
                (->> (update result :inputs (into #{} (map name) (:inputs result)))
                     (assoc components component-name))
                (remove-from-components components component-name))))))

(comment
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
      (unlink "x2" "sink2")
      (link "x5" "x2")
      ->config))
