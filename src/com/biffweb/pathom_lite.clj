(ns com.biffweb.pathom-lite
  "A lightweight implementation of pathom-style resolvers.

  Supports:
  - Simple resolvers with declared input/output
  - Nested queries (joins)
  - Nested inputs (resolvers that require sub-attributes of their inputs)
  - Optional inputs ([:? :key] syntax)
  - Optional query items ([:? :key] in query vectors)
  - Global resolvers (no input)
  - Var-based resolvers (metadata-driven)
  - Batch resolvers (process multiple entities at once, breadth-first)
  - Strict mode only (throws on missing data)

  Omits (compared to pathom3):
  - Plugin system
  - Lenient mode
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
  - Uses var metadata for :input, :output, and :batch
  - Derives :id from the var's namespace and name
  - Stores the var itself (not the deref'd fn) as :resolve

  When given a map, expects:
    :id      - keyword, unique resolver id
    :input   - vector of input specs (keywords, join maps, or optional wrappers)
    :output  - vector of flat keywords this resolver provides
    :resolve - (fn [ctx input-map] output-map), or if :batch is true,
               (fn [ctx [input-map ...]] [output-map ...])
    :batch   - (optional) if true, the resolver takes a vector of input maps
               and returns a vector of output maps in the same order

  Returns a resolver map with keys :id, :input, :output, :resolve, :batch."
  [resolver-or-map]
  (if (var? resolver-or-map)
    (let [var-meta (meta resolver-or-map)
          id (keyword (str (:ns var-meta)) (str (:name var-meta)))
          input (or (:input var-meta) [])
          output (or (:output var-meta) [])
          batch (boolean (:batch var-meta))]
      (when (some map? output)
        (throw (ex-info "Resolver :output must be flat keywords, not nested maps"
                        {:resolver id :output output})))
      {:id id
       :input input
       :output output
       :resolve resolver-or-map
       :batch batch})
    (let [{:keys [id input output resolve batch]} resolver-or-map]
      (when-not id
        (throw (ex-info "Resolver must have an :id" {:resolver resolver-or-map})))
      (when-not resolve
        (throw (ex-info "Resolver must have a :resolve function" {:resolver resolver-or-map})))
      (when (some map? output)
        (throw (ex-info "Resolver :output must be flat keywords, not nested maps"
                        {:resolver id :output output})))
      {:id id
       :input (or input [])
       :output (or output [])
       :resolve resolve
       :batch (boolean batch)})))

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
(declare ^:private process-entities)
(declare ^:private resolve-value)
(declare ^:private resolve-input-map)

(defn- find-resolver-candidates
  "Find all resolvers that can provide `attr`."
  [ctx attr]
  (get-in (:biff.pathom-lite/index ctx) [:resolvers-by-output attr]))

(defn- ensure-join-value
  "Validate that a value is suitable for a join (map or sequential of maps).
  Throws if the value is nil or a scalar."
  [v attr context]
  (when (or (nil? v) (not (or (map? v) (sequential? v))))
    (throw (ex-info (str "Expected a map or collection for join on " attr
                         ", but got: " (pr-str v))
                    {:attr attr :value v :context context}))))

(defn- apply-sub-query
  "Apply a sub-query to a resolved value. The value must be a map or collection of maps.
  For sequential values, uses breadth-first batch processing via process-entities."
  [ctx v attr sub-query]
  (ensure-join-value v attr :query)
  (if (map? v)
    (process-query ctx v sub-query)
    (process-entities ctx v sub-query)))

(defn- resolve-value
  "Resolve a raw value for attr from resolver candidates (not from entity).
  `resolving` is a set of attrs currently being resolved (cycle detection).
  Throws with ::resolve-error on ex-data if resolution fails.
  For batch resolvers in single-entity context, wraps input in a vector and unwraps."
  [ctx entity attr resolving]
  (if (contains? resolving attr)
    (throw (ex-info (str "Cycle detected while resolving " attr)
                    {::resolve-error true :attr attr :resolving resolving}))
    (let [resolving (conj resolving attr)
          candidates (find-resolver-candidates ctx attr)
          resolved (some
                    (fn [r]
                      (try
                        (let [input-map (resolve-input-map ctx entity (:input r) resolving)
                              result (if (:batch r)
                                       (first ((:resolve r) ctx [input-map]))
                                       ((:resolve r) ctx input-map))]
                          (when (contains? result attr)
                            {:value (get result attr)}))
                        (catch clojure.lang.ExceptionInfo e
                          (if (::resolve-error (ex-data e))
                            nil
                            (throw e)))))
                    candidates)]
      (if resolved
        (:value resolved)
        (throw (ex-info (str "No resolver found for attribute " attr
                             " with available inputs " (keys entity))
                        {::resolve-error true :attr attr :available-keys (keys entity)}))))))

(defn- resolve-attr
  "Resolve a single attribute for the given entity, optionally applying a sub-query.
  First gets the raw value (from entity or resolvers), then applies sub-query if present."
  [ctx entity attr sub-query resolving]
  (let [v (if (contains? entity attr)
            (get entity attr)
            (resolve-value ctx entity attr resolving))]
    (if sub-query
      (apply-sub-query ctx v attr sub-query)
      v)))

(defn- resolve-input-map
  "Resolve all required inputs and build the input map in a single pass.
  For each input item: resolves the key if missing, then handles sub-inputs."
  [ctx entity input resolving]
  (reduce
   (fn [result input-item]
     (let [optional? (input-item-optional? input-item)
           normalized (normalize-input-item input-item)
           [k sub-input] (if (map? normalized)
                           [(ffirst normalized) (val (first normalized))]
                           [normalized nil])
           resolve-item
           (fn []
             (let [raw (if (contains? entity k)
                         (get entity k)
                         (resolve-value ctx entity k resolving))]
               (if sub-input
                 (do (ensure-join-value raw k :input)
                     (if (map? raw)
                       (resolve-input-map ctx raw sub-input resolving)
                       (mapv #(resolve-input-map ctx % sub-input resolving) raw)))
                 raw)))]
       (if optional?
         (try
           (assoc result k (resolve-item))
           (catch clojure.lang.ExceptionInfo e
             (if (::resolve-error (ex-data e))
               result
               (throw e))))
         (assoc result k (resolve-item)))))
   {}
   input))

(defn- normalize-query-item
  "Unwrap optional markers from a query item.
  Returns [attr sub-query optional?]."
  [query-item]
  (cond
    ;; [:? :keyword] or [:? {:keyword [...]}]
    (optional-input? query-item)
    (let [inner (unwrap-optional query-item)
          [attr sub-query _] (normalize-query-item inner)]
      [attr sub-query true])

    ;; {[:? :keyword] [...]} — optional join
    (and (map? query-item) (optional-input? (ffirst query-item)))
    [(unwrap-optional (ffirst query-item)) (val (first query-item)) true]

    ;; {:keyword [...]} — regular join
    (map? query-item)
    [(ffirst query-item) (val (first query-item)) false]

    ;; :keyword — plain attribute
    :else
    [query-item nil false]))

;; ---------------------------------------------------------------------------
;; Breadth-first batch processing
;; ---------------------------------------------------------------------------

(defn- resolve-attrs-batch
  "Resolve a single attr for multiple entities, using batch resolvers when available.
  Returns a vector of values, one per entity. Uses ::unresolved sentinel for
  optional items that could not be resolved."
  [ctx entities attr optional?]
  (let [candidates (find-resolver-candidates ctx attr)
        batch-r (first (filter :batch candidates))]
    (if batch-r
      ;; Batch path: separate entities that already have attr from those that need it
      (let [has-attr? (mapv #(contains? % attr) entities)
            need-idxs (vec (keep-indexed (fn [i has?] (when-not has? i)) has-attr?))]
        (if (empty? need-idxs)
          (mapv #(get % attr) entities)
          (let [input-maps
                (try
                  (mapv (fn [i] (resolve-input-map ctx (nth entities i) (:input batch-r) #{}))
                        need-idxs)
                  (catch clojure.lang.ExceptionInfo e
                    (if (and optional? (::resolve-error (ex-data e)))
                      ::skip
                      (throw e))))

                batch-results
                (when (not= input-maps ::skip)
                  ((:resolve batch-r) ctx input-maps))

                idx->batch-idx
                (when batch-results
                  (into {} (map-indexed (fn [bi ei] [ei bi]) need-idxs)))]
            (mapv
             (fn [i entity]
               (cond
                 (contains? entity attr)
                 (get entity attr)

                 (and idx->batch-idx
                      (contains? idx->batch-idx i)
                      (contains? (nth batch-results (get idx->batch-idx i)) attr))
                 (get (nth batch-results (get idx->batch-idx i)) attr)

                 :else
                 (if optional?
                   (try (resolve-value ctx entity attr #{})
                        (catch clojure.lang.ExceptionInfo e
                          (if (::resolve-error (ex-data e)) ::unresolved (throw e))))
                   (resolve-value ctx entity attr #{}))))
             (range) entities))))
      ;; No batch resolver: individual resolution
      (mapv (fn [entity]
              (if (contains? entity attr)
                (get entity attr)
                (if optional?
                  (try (resolve-value ctx entity attr #{})
                       (catch clojure.lang.ExceptionInfo e
                         (if (::resolve-error (ex-data e)) ::unresolved (throw e))))
                  (resolve-value ctx entity attr #{}))))
            entities))))

(defn- process-entities
  "Process a query against multiple entities using breadth-first traversal.
  For each query item, resolves it across ALL entities before moving to the next.
  For joins with sequential values, flattens all children across all parents
  and processes them in a single recursive call. This means a batch resolver
  at depth N is called exactly once for all entities at that depth, regardless
  of how many parents exist at depth N-1."
  [ctx entities query-vec]
  (if (empty? entities)
    []
    (reduce
     (fn [results query-item]
       (let [[attr sub-query optional?] (normalize-query-item query-item)
             enriched (mapv merge entities results)
             values (resolve-attrs-batch ctx enriched attr optional?)]
         (if sub-query
           ;; Join: collect all children across all parents, process recursively, reassemble
           (let [child-types (mapv (fn [v]
                                     (cond
                                       (= v ::unresolved)  :unresolved
                                       (map? v)            :map
                                       (sequential? v)     :seq
                                       :else               (do (ensure-join-value v attr :query)
                                                               :invalid)))
                                   values)
                 ;; Flatten all children into a single list for batch processing
                 all-children (into []
                                    (mapcat (fn [v t]
                                              (case t
                                                :map [v]
                                                :seq v
                                                []))
                                            values child-types))
                 ;; Recursively process all children in one batch
                 processed (if (seq all-children)
                             (vec (process-entities ctx all-children sub-query))
                             [])
                 ;; Reassemble results back into parent structure
                 final (loop [rs (transient results)
                              idx 0
                              offset 0]
                         (if (>= idx (count results))
                           (persistent! rs)
                           (let [t (nth child-types idx)]
                             (case t
                               :unresolved
                               (recur rs (inc idx) offset)

                               :map
                               (recur (assoc! rs idx (assoc (nth results idx) attr (nth processed offset)))
                                      (inc idx)
                                      (inc offset))

                               :seq
                               (let [n (count (nth values idx))
                                     children (subvec processed offset (+ offset n))]
                                 (recur (assoc! rs idx (assoc (nth results idx) attr children))
                                        (inc idx)
                                        (+ offset n)))))))]
             final)
           ;; No sub-query: store values directly
           (mapv (fn [result v]
                   (if (= v ::unresolved)
                     result
                     (assoc result attr v)))
                 results values))))
     (vec (repeat (count entities) {}))
     query-vec)))

(defn- process-query
  "Process an EQL query against the given entity using the resolver index.
  Supports optional query items via [:? ...] syntax.
  Returns a map of the requested attributes."
  [ctx entity query-vec]
  (reduce
   (fn [result query-item]
     (let [[attr sub-query optional?] (normalize-query-item query-item)
           enriched-entity (merge entity result)]
       (if optional?
         (try
           (assoc result attr (resolve-attr ctx enriched-entity attr sub-query #{}))
           (catch clojure.lang.ExceptionInfo e
             (if (::resolve-error (ex-data e))
               result
               (throw e))))
         (assoc result attr (resolve-attr ctx enriched-entity attr sub-query #{})))))
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
             Supports optional items via [:? :attr] syntax.

  Returns a map satisfying the query."
  [{:keys [biff.pathom-lite/index] :as ctx} entity query-vec]
  (process-query ctx (or entity {}) query-vec))
