(ns biff.pl
  "A lightweight implementation of pathom-style resolvers.

  Supports:
  - Simple resolvers with declared input/output
  - Nested queries (joins)
  - Nested inputs (resolvers that require sub-attributes of their inputs)
  - Optional inputs ([:? :key] syntax)
  - Global resolvers (no input)
  - Var-based resolvers (metadata-driven)
  - Strict mode only (throws on missing data)

  Omits (compared to pathom3):
  - Plugin system
  - Lenient mode
  - Batch resolvers
  - Query planning (uses query directly)
  - EQL AST manipulation")

;; ---------------------------------------------------------------------------
;; Input helpers
;; ---------------------------------------------------------------------------

(defn- optional-input?
  "Returns true if input-item is an optional input marker [:? ...]."
  [input-item]
  (and (vector? input-item)
       (= :? (first input-item))))

(defn- unwrap-optional
  "Given an optional input marker [:? x], returns x."
  [input-item]
  (second input-item))

(defn- input-item-key
  "Extract the top-level key from an input item (keyword, join map, or optional wrapper)."
  [input-item]
  (cond
    (optional-input? input-item) (input-item-key (unwrap-optional input-item))
    (map? input-item) (let [k (ffirst input-item)]
                        (if (optional-input? k)
                          (input-item-key (unwrap-optional k))
                          k))
    :else input-item))

(defn- input-item-optional?
  "Returns true if the input item is optional (either [:? :key] or {[:? :key] [...]})."
  [input-item]
  (or (optional-input? input-item)
      (and (map? input-item)
           (optional-input? (ffirst input-item)))))

(defn- normalize-input-item
  "Unwrap optional markers from an input item for processing.
  E.g. [:? :foo] -> :foo, {[:? :bar] [...]} -> {:bar [...]}"
  [input-item]
  (cond
    (optional-input? input-item) (unwrap-optional input-item)
    (and (map? input-item) (optional-input? (ffirst input-item)))
    {(unwrap-optional (ffirst input-item)) (val (first input-item))}
    :else input-item))

;; ---------------------------------------------------------------------------
;; Registry helpers
;; ---------------------------------------------------------------------------

(defn resolver
  "Define a resolver. Accepts either a map or a var.

  When given a var:
  - Uses var metadata for :input and :output
  - Derives :id from the var's namespace and name
  - Stores the var itself (not the deref'd fn) as :resolver

  When given a map, expects:
    :id       - keyword, unique resolver id
    :input    - vector of input specs (keywords, join maps, or optional wrappers)
    :output   - vector of flat keywords this resolver provides
    :resolver - (fn [ctx input-map] output-map)

  Returns a resolver map with keys :id, :input, :output, :resolver."
  [resolver-or-map]
  (if (var? resolver-or-map)
    (let [var-meta (meta resolver-or-map)
          id (keyword (str (:ns var-meta)) (str (:name var-meta)))
          input (or (:input var-meta) [])
          output (or (:output var-meta) [])]
      (when (some map? output)
        (throw (ex-info "Resolver :output must be flat keywords, not nested maps"
                        {:resolver id :output output})))
      {:id id
       :input input
       :output output
       :resolver resolver-or-map})
    (let [{:keys [id input output resolver]} resolver-or-map]
      (when-not id
        (throw (ex-info "Resolver must have an :id" {:resolver resolver-or-map})))
      (when-not resolver
        (throw (ex-info "Resolver must have a :resolver function" {:resolver resolver-or-map})))
      (when (some map? output)
        (throw (ex-info "Resolver :output must be flat keywords, not nested maps"
                        {:resolver id :output output})))
      {:id id
       :input (or input [])
       :output (or output [])
       :resolver resolver})))

(defn build-index
  "Build an index from a collection of resolvers (maps or vars).
  Calls `resolver` on each item.
  Returns a map with:
    :resolvers-by-output  {attr-key [resolver ...]}
    :all-resolvers        [resolver ...]"
  [resolvers]
  (let [resolvers (mapv resolver resolvers)]
    {:resolvers-by-output
     (reduce (fn [idx r]
               (reduce (fn [idx k]
                         (update idx k (fnil conj []) r))
                       idx
                       (:output r)))
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

(defn- ensure-join-value
  "Validate that a value is suitable for a join (map or sequential of maps).
  Throws if the value is nil or a scalar."
  [v attr context]
  (when (or (nil? v) (not (or (map? v) (sequential? v))))
    (throw (ex-info (str "Expected a map or collection for join on " attr
                         ", but got: " (pr-str v))
                    {:attr attr :value v :context context}))))

(defn- invoke-resolver
  "Invoke a resolver's function. Handles both vars and plain functions."
  [r ctx input-map]
  (let [f (:resolver r)]
    (if (var? f)
      (@f ctx input-map)
      (f ctx input-map))))

(defn- resolve-input-map
  "Resolve all required inputs and build the (possibly nested) input map.
  Handles both flat keyword inputs, nested join inputs, and optional inputs."
  [ctx index entity input resolving]
  (let [enriched (reduce
                  (fn [ent input-item]
                    (let [k (input-item-key input-item)
                          optional? (input-item-optional? input-item)]
                      (if (contains? ent k)
                        ent
                        (if optional?
                          (try
                            (assoc ent k (resolve-attr ctx index ent k nil resolving))
                            (catch clojure.lang.ExceptionInfo _e
                              ent))
                          (assoc ent k (resolve-attr ctx index ent k nil resolving))))))
                  entity
                  input)]
    (reduce
     (fn [result input-item]
       (let [optional? (input-item-optional? input-item)
             normalized (normalize-input-item input-item)]
         (if (map? normalized)
           (let [attr      (ffirst normalized)
                 sub-input (val (first normalized))
                 v         (get enriched attr)]
             (if (and optional? (nil? v))
               result
               (do
                 (ensure-join-value v attr :input)
                 (assoc result attr
                        (if (map? v)
                          (resolve-input-map ctx index v sub-input resolving)
                          (mapv #(resolve-input-map ctx index % sub-input resolving) v))))))
           (let [v (get enriched normalized ::not-found)]
             (if (and optional? (= v ::not-found))
               result
               (if (= v ::not-found)
                 (throw (ex-info (str "Required input " normalized " not found")
                                 {:input normalized :available-keys (keys enriched)}))
                 (assoc result normalized v)))))))
     {}
     input)))

(defn- resolve-attr
  "Resolve a single attribute (possibly a join) for the given entity.
  `resolving` is a set of attrs currently being resolved (cycle detection)."
  [ctx index entity attr sub-query resolving]
  (if (contains? entity attr)
    (let [v (get entity attr)]
      (if sub-query
        (do
          (ensure-join-value v attr :query)
          (if (map? v)
            (process-query ctx index v sub-query)
            (mapv #(process-query ctx index % sub-query) v)))
        v))
    (if (contains? resolving attr)
      (throw (ex-info (str "Cycle detected while resolving " attr)
                      {:attr attr :resolving resolving}))
      (let [resolving (conj resolving attr)
            candidates (find-resolver-candidates index attr)
            resolved (some
                      (fn [r]
                        (try
                          (let [input-map (resolve-input-map ctx index entity (:input r) resolving)
                                result (invoke-resolver r ctx input-map)]
                            (when (contains? result attr)
                              {:value (get result attr)}))
                          (catch clojure.lang.ExceptionInfo _e
                            nil)))
                      candidates)]
        (if resolved
          (let [v (:value resolved)]
            (if sub-query
              (do
                (ensure-join-value v attr :query)
                (if (map? v)
                  (process-query ctx index v sub-query)
                  (mapv #(process-query ctx index % sub-query) v)))
              v))
          (throw (ex-info (str "No resolver found for attribute " attr
                               " with available inputs " (keys entity))
                          {:attr attr :available-keys (keys entity)})))))))

(defn- process-query
  "Process an EQL query against the given entity using the resolver index.
  Returns a map of the requested attributes."
  [ctx index entity query-vec]
  (reduce
   (fn [result query-item]
     (let [[attr sub-query] (if (map? query-item)
                              [(ffirst query-item) (val (first query-item))]
                              [query-item nil])
           enriched-entity (merge entity result)
           v (resolve-attr ctx index enriched-entity attr sub-query #{})]
       (assoc result attr v)))
   {}
   query-vec))

;; ---------------------------------------------------------------------------
;; Public API
;; ---------------------------------------------------------------------------

(defn query
  "Run an EQL query using the provided resolver index.

  Arguments:
    ctx    - context map; must include :biff.pathom-lite/index (from build-index).
             Any other keys are passed through to resolver functions.
    entity - initial entity map with seed data (or {})
    query  - EQL query vector, e.g. [:user/name {:user/friends [:user/name]}]

  Returns a map satisfying the query."
  [{:keys [biff.pathom-lite/index] :as ctx} entity query-vec]
  (process-query ctx index (or entity {}) query-vec))
