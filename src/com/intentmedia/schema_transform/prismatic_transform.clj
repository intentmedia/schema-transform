(ns com.intentmedia.schema-transform.prismatic-transform
  (:require [schema.core :as s]
            [camel-snake-kebab.core :refer [->PascalCase ->snake_case]])
  (:import [schema.core EnumSchema Maybe Both]))

(declare avro-type to-avro)

(def primitives
  {Boolean "boolean"
   Double  "double"
   Float   "float"
   Integer "int"
   Long    "long"
   Number  "double"
   String  "string"})

(defn primitive-type [schema]
  (primitives schema))

(defn map-type [schema]
  {:type   "map"
   :values (primitive-type (first (vals schema)))})

(def null-type "null")

(defn build-meta [field-name schema namespace]
  {:name      (->PascalCase
                (str (or (s/schema-name schema)
                         (name field-name))))
   :namespace (str (or (s/schema-ns schema) namespace))})

(defn both-type [field-name schema namespace schemas]
  (avro-type schemas
             (first (:schemas schema))
             (build-meta field-name schema namespace)))

(defn avro-field
  [schemas schema [namespace field-name]]
  (let [field-name* (-> (if (s/optional-key? field-name)
                          (:k field-name)
                          field-name)
                        name
                        ->snake_case)
        type (avro-type schemas
                        schema
                        (build-meta field-name*
                                    schema
                                    namespace))]
    {:type type
     :name field-name*}))

(defn add-to-schemas [schemas avro-schema]
  (swap! schemas assoc (str (:namespace avro-schema) "." (:name avro-schema))
         avro-schema))

(defn record-type [field-name schema namespace schemas]
  (let [fields (mapv (partial avro-field schemas)
                     (vals schema)
                     (map (partial vector namespace)
                          (keys schema)))
        avro-schema (-> {:type   "record"
                         :fields fields}
                        (merge (build-meta field-name
                                           schema
                                           namespace)))]
    (add-to-schemas schemas avro-schema)
    (:name avro-schema)))


(defn enum-type [field-name schema namespace schemas]
  (let [avro-schema (-> {:type    "enum"
                         :symbols (vec (:vs schema))}
                        (merge (build-meta field-name
                                           schema
                                           namespace)))]
    (add-to-schemas schemas avro-schema)
    (:name avro-schema)))

(defn optional-type [field-name schema namespace schemas]
  ["null" (avro-type schemas
                     (:schema schema)
                     (build-meta field-name schema namespace))])

(defn array-type [field-name schema namespace schemas]
  {:type  "array"
   :items (avro-type schemas
                     (first schema)
                     (build-meta (str (name field-name)
                                      "Item")
                                 schema
                                 namespace))})

(defn primitive? [schema]
  (contains? primitives schema))

(defn enum? [schema]
  (= EnumSchema (class schema)))

(defn record?* [schema]
  (and (map? schema) (keyword? (first (first schema)))))

(defn map?* [schema]
  (and (map? schema)
       (= (count schema) 1)
       (= (first (keys schema)) String)))

(defn both? [schema]
  (= (class schema) Both))

(defn optional? [field-name schema]
  (or (s/optional-key? field-name)
      (= (type schema) Maybe)))

(defn avro-type
  [schemas schema & [{:keys [name namespace]}]]
  (cond
    (optional? name schema) (optional-type name schema namespace schemas)
    (primitive? schema) (primitive-type schema)
    (nil? schema) null-type
    (sequential? schema) (array-type name schema namespace schemas)
    (map?* schema) (map-type schema)
    (both? schema) (both-type name schema namespace schemas)
    (enum? schema) (enum-type name schema namespace schemas)
    (record?* schema) (record-type name schema namespace schemas)
    :else (throw (RuntimeException. (str "Could not tranform field "
                                         name
                                         " with schema "
                                         schema)))))

(defn to-avro
  [prismatic-schema & {:keys [name namespace]}]
  (let [schemas (atom {})]
    (avro-type schemas
               prismatic-schema
               (build-meta name
                           prismatic-schema
                           namespace))
    (-> @schemas vals vec)))