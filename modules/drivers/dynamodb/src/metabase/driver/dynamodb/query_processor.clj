(ns metabase.driver.dynamodb.query-processor
  (:require [clojure.string :as str]
            [metabase.mbql
             [schema :as mbql.s]
             [util :as mbql.u]]
            [metabase.models.field :refer [Field]]
            [metabase.query-processor
             [interface :as i]
             [store :as qp.store]]
            [metabase.util.i18n :refer [tru]]
            [metabase.query-processor.util.add-alias-info :as add]
            [metabase.query-processor.error-type :as qp.error-type]
            [metabase.driver.dynamodb.util :refer [*dynamodb-client*]]))

(def ^:dynamic ^:private *query* nil)

(defn list-tables []
  (-> (.listTables *dynamodb-client*)
      (.getTableNames)))

(defn- dynamodb-type->base-type [attr-type]
  (case attr-type
    "N"      :type/Decimal
    "S"      :type/Text
    "BOOL"   :type/Boolean
    :type/*))

(defn describe-table [table]
  (let [table-desc (-> (.describeTable *dynamodb-client* table)
                       (.getTable))]
    (for [attribute-def (.getAttributeDefinitions table-desc)]
      {:name      (.getAttributeName attribute-def)
       :database-type (.getAttributeType attribute-def)
       :base-type (dynamodb-type->base-type (.getAttributeType attribute-def))
       :database-position 0})))

;;
;;
;;
(defmulti ^:private ->rvalue
  "Format this `Field` or value for use as the right hand value of an expression, e.g. by adding `$` to a `Field`'s
  name"
  {:arglists '([x])}
  mbql.u/dispatch-by-clause-name-or-class)

(defmulti ^:private ->lvalue
  "Return an escaped name that can be used as the name of a given Field."
  {:arglists '([field])}
  mbql.u/dispatch-by-clause-name-or-class)

(defmulti ^:private ->initial-rvalue
  "Return the rvalue that should be used in the *initial* projection for this `Field`."
  {:arglists '([field])}
  mbql.u/dispatch-by-clause-name-or-class)


(defn- field->name
  "Return a single string name for FIELD. For nested fields, this creates a combined qualified name."
  ^String [^:-i/FieldInstance field, ^String separator]
  (if-let [parent-id (:parent_id field)]
    (str/join separator [(field->name (qp.store/field parent-id) separator)
                         (:name field)])
    (:name field)))

(defmethod ->lvalue         (class Field) [this] (field->name this "___"))
(defmethod ->initial-rvalue (class Field) [this] (str \$ (field->name this ".")))
(defmethod ->rvalue         (class Field) [this] (str \$ (->lvalue this)))

(defmethod ->lvalue Field
  [field]
  (field->name field "\n"))

(defmethod ->rvalue Field
  [{coercion :coercion_strategy, ::keys [source-alias join-field] :as field}]
  (let [field-name (str \$ (cond->> (or source-alias (field->name field "\n"))
                             join-field (str join-field \.)))]
    (cond
      (isa? coercion :Coercion/UNIXMilliSeconds->DateTime)
      {:$dateFromParts {:millisecond field-name, :year 1970, :timezone "UTC"}}

      (isa? coercion :Coercion/UNIXSeconds->DateTime)
      {:$dateFromParts {:second field-name, :year 1970, :timezone "UTC"}}

      (isa? coercion :Coercion/YYYYMMDDHHMMSSString->Temporal)
      {"$dateFromString" {:dateString field-name
                          :format     "%Y%m%d%H%M%S"
                          :onError    field-name}}

      ;; mongo only supports datetime
      (isa? coercion :Coercion/ISO8601->DateTime)
      {"$dateFromString" {:dateString field-name
                          :onError    field-name}}


      (isa? coercion :Coercion/ISO8601->Date)
      (throw (ex-info (tru "MongoDB does not support parsing strings as dates. Try parsing to a datetime instead")
                      {:type              qp.error-type/unsupported-feature
                       :coercion-strategy coercion}))


      (isa? coercion :Coercion/ISO8601->Time)
      (throw (ex-info (tru "MongoDB does not support parsing strings as times. Try parsing to a datetime instead")
                      {:type              qp.error-type/unsupported-feature
                       :coercion-strategy coercion}))

      :else field-name)))


(defn- get-join-alias
  "Calculates the name of the join field used for `join-alias`, if any.
  It is assumed that join aliases are unique in the query (this is ensured by the escape-join-aliases middleware),
  so the alias is simply prefixed with a string to make it less likely that join filed we introduce in the $unwind
  stage overwrites a field of the document being joined to."
  [join-alias]
  (some->> join-alias (str "join_alias_")))

(def ^:dynamic ^:private *field-mappings*
  "The mapping from the fields to the projected names created
  by the nested query."
  {})

(defn- find-mapped-field-name
  "Finds the name of a mapped field, if any.
  First it does a quick exact match and if the field is not found, it searches for a field with the same ID/name and
  the same join alias.
  Note that during the compilation of joins, the field :join-alias is renamed to ::join-local to prevent prefixing the
  fields of the current join to be prefixed with the join alias."
  [[_ field-id params :as field]]
  (or (get *field-mappings* field)
      (some (fn [[e n]]
              (when (and (vector? e)
                         (= (subvec e 0 2) [:field field-id])
                         (= (:join-alias (e 2)) (:join-alias params))
                         (= (::join-local (e 2)) (::join-local params)))
                n))
            *field-mappings*)))

(declare with-rvalue-temporal-bucketing)

(defmethod ->lvalue         :field-id [[_ field-id]] (->lvalue         (qp.store/field field-id)))
(defmethod ->initial-rvalue :field-id [[_ field-id]] (->initial-rvalue (qp.store/field field-id)))
(defmethod ->rvalue         :field-id [[_ field-id]] (->rvalue         (qp.store/field field-id)))

(defmethod ->lvalue         :field [[_ field]] (->lvalue         (qp.store/field field)))
(defmethod ->rvalue :field
  [[_ id-or-name {:keys [temporal-unit join-alias] ::add/keys [source-alias]} :as field]]
  (let [join-field (get-join-alias join-alias)]
    (cond-> (if (integer? id-or-name)
              (if-let [mapped (find-mapped-field-name field)]
                (str \$ mapped)
                (->rvalue (assoc (qp.store/field id-or-name)
                                 ::source-alias source-alias
                                 ::join-field join-field)))
              (if-let [mapped (find-mapped-field-name field)]
                (str \$ mapped)
                (str \$ (cond->> (or source-alias (name id-or-name))
                          join-field (str join-field \.)))))
      temporal-unit (with-rvalue-temporal-bucketing temporal-unit))))

(defn- handle-fields [{:keys [fields]} pipeline-ctx]
  (println "handle-fields" fields)
  (if-not (seq fields)
    pipeline-ctx
    (let [new-projections (for [field fields]
                            [(->lvalue field) (->rvalue field)])]
      (-> pipeline-ctx
          (assoc :projections (map (comp keyword first) new-projections))
          (update :query conj (into {} new-projections))))))

(defn mbql->native [{{source-table-id :source-table} :query, :as query}]
  (let [{source-table-name :name} (qp.store/table source-table-id)]
    (binding [*query* query]
      (println "mbql->native:" query)
      {:projections nil
       :query       (reduce (fn [pipeline-ctx f]
                              (f (:query query) pipeline-ctx))
                            {:projections [], :query []}
                            [handle-fields])
       :collection  nil
       :mbql?       true})))

(defn execute-reducible-query
  [{{:keys [collection query mbql? projections]} :native}]
  (println "execute-reducible-query:"  query)
  {:rows []})