(ns com.biffweb.pathom-lite
  "A lightweight implementation of pathom-style resolvers.

  Supports:
  - Simple resolvers with declared input/output
  - Nested queries (joins)
  - Global resolvers (no input)
  - Strict mode only (throws on missing data)

  Omits (compared to pathom3):
  - Plugin system
  - Lenient mode
  - Batch resolvers
  - Query planning (uses query directly)
  - EQL AST manipulation
  - Optional resolvers")

;; ---------------------------------------------------------------------------
;; Registry helpers
;; ---------------------------------------------------------------------------

(defn resolver
  "Define a resolver. Returns a resolver map.

  Options:
    :name     - keyword, unique resolver name
    :input    - vector of keywords this resolver requires
    :output   - vector of keywords (or nested maps) this resolver provides
    :resolve  - (fn [env input-map] output-map)"
  [{:keys [name input output resolve] :as opts}]
  (when-not name
    (throw (ex-info "Resolver must have a :name" {:resolver opts})))
  (when-not resolve
    (throw (ex-info "Resolver must have a :resolve function" {:resolver opts})))
  (assoc opts
         :input  (or input [])
         :output (or output [])))

(defn- flatten-output-keys
  "Extract the top-level attribute keys from an output declaration.
  Output items can be plain keywords or single-entry maps (for joins)."
  [output]
  (mapv (fn [item]
          (if (map? item)
            (ffirst item)
            item))
        output))

(defn build-index
  "Build an index from a collection of resolvers.
  Returns a map with:
    :resolvers-by-output  {attr-key [resolver ...]}
    :all-resolvers        [resolver ...]"
  [resolvers]
  (let [resolvers (mapv (fn [r] (if (:resolve r) r (resolver r))) resolvers)]
    {:resolvers-by-output
     (reduce (fn [idx r]
               (let [out-keys (flatten-output-keys (:output r))]
                 (reduce (fn [idx k]
                           (update idx k (fnil conj []) r))
                         idx
                         out-keys)))
             {}
             resolvers)
     :all-resolvers resolvers}))

;; ---------------------------------------------------------------------------
;; Query engine
;; ---------------------------------------------------------------------------

(declare process-query)
(declare ^:private resolve-attr)

(defn- find-resolver-candidates
  "Find all resolvers that can provide `attr`."
  [index attr]
  (get-in index [:resolvers-by-output attr]))

(defn- output-spec-for-attr
  "Given a resolver's output declaration, find the nested spec for `attr`.
  Returns nil for plain attributes, or the nested query vector for joins."
  [output attr]
  (some (fn [item]
          (when (and (map? item) (contains? item attr))
            (get item attr)))
        output))

(defn- ensure-inputs
  "Ensure all required input keys are present in entity, resolving them
  transitively if needed. Returns the enriched entity."
  [env index entity required-keys resolving]
  (reduce
   (fn [ent k]
     (if (contains? ent k)
       ent
       (let [v (resolve-attr env index ent k nil resolving)]
         (assoc ent k v))))
   entity
   required-keys))

(defn- resolve-attr
  "Resolve a single attribute (possibly a join) for the given entity.
  `resolving` is a set of attrs currently being resolved (cycle detection)."
  [env index entity attr sub-query resolving]
  (if (contains? entity attr)
    ;; Already present — but if there's a sub-query we need to process children
    (let [v (get entity attr)]
      (if sub-query
        (cond
          (map? v)        (process-query env index v sub-query)
          (sequential? v) (mapv #(process-query env index % sub-query) v)
          :else           v)
        v))
    ;; Cycle detection
    (if (contains? resolving attr)
      (throw (ex-info (str "Cycle detected while resolving " attr)
                      {:attr attr :resolving resolving}))
      (let [resolving (conj resolving attr)
            candidates (find-resolver-candidates index attr)
            ;; Try each candidate; pick the first one whose inputs we can satisfy
            resolved (some
                      (fn [r]
                        (try
                          (let [enriched (ensure-inputs env index entity (:input r) resolving)
                                input-map (select-keys enriched (:input r))
                                result ((:resolve r) env input-map)
                                v (get result attr)]
                            {:value v :enriched enriched})
                          (catch clojure.lang.ExceptionInfo _e
                            nil)))
                      candidates)]
        (if resolved
          (let [v (:value resolved)]
            (if (and sub-query v)
              (cond
                (map? v)        (process-query env index v sub-query)
                (sequential? v) (mapv #(process-query env index % sub-query) v)
                :else           v)
              v))
          (throw (ex-info (str "No resolver found for attribute " attr
                               " with available inputs " (keys entity))
                          {:attr attr :available-keys (keys entity)})))))))

(defn- process-query
  "Process an EQL query against the given entity using the resolver index.
  Returns a map of the requested attributes."
  [env index entity query]
  (reduce
   (fn [result query-item]
     (let [[attr sub-query] (if (map? query-item)
                              [(ffirst query-item) (val (first query-item))]
                              [query-item nil])
           ;; Merge already-resolved data into entity so subsequent resolvers
           ;; can use attributes resolved earlier in this query.
           enriched-entity (merge entity result)
           v (resolve-attr env index enriched-entity attr sub-query #{})]
       (assoc result attr v)))
   {}
   query))

;; ---------------------------------------------------------------------------
;; Public API
;; ---------------------------------------------------------------------------

(defn process
  "Run an EQL query using the provided resolvers.

  Options:
    :resolvers - collection of resolver maps (from `resolver`)
    :query     - EQL query vector, e.g. [:user/name {:user/friends [:friend/name]}]
    :entity    - (optional) initial entity map with seed data
    :env       - (optional) environment map passed to resolver fns

  Returns a map satisfying the query."
  [{:keys [resolvers query entity env]}]
  (let [index (build-index resolvers)
        entity (or entity {})]
    (process-query (or env {}) index entity query)))
