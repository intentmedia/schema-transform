(ns com.intentmedia.schema-transform.prismatic-transform
  (:require [cheshire.core :refer [parse-string generate-string]]
            [schema.core :as s]
            [camel-snake-kebab.core :refer [->PascalCase ->snake_case]])
  (:import [schema.core EnumSchema Maybe Both]))

(defrecord Optional [schema])

(def primitives
  {Boolean "boolean"
   Double  "double"
   Float   "float"
   Integer "int"
   Long    "long"
   Number  "double"
   String  "string"})

(declare avro-field avro-type nested-type)

(defn primitive-type [schema]
  (primitives schema))

(defn map-type [schema]
  {:type   "map"
   :values (primitive-type (first (vals schema)))})

(def null-type "null")

(defn both-type [field-name schema]
  (avro-type field-name (first (:schemas schema))))

(defn record-type [field-name schema]
  (let [fields (mapv avro-field (keys schema) (vals schema))]
    {:type   "record"
     :name   (->PascalCase
               (or (s/schema-name schema)
                   field-name))
     :fields fields}))

(defn optional-type [field-name schema]
  ["null" (avro-type field-name (:schema schema))])

(defn enum-type [field-name schema]
  {:type      "enum"
   :symbols   (vec (:vs schema))
   :name      (->PascalCase
                (or (s/schema-name schema)
                    field-name))
   :namespace (s/schema-ns schema)})

(defn array-type [field-name schema]
  {:type  "array"
   :items (avro-type field-name (first schema))})

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
  [field-name schema]
  (cond
    (optional? field-name schema) (optional-type field-name schema)
    (primitive? schema) (primitive-type schema)
    (nil? schema) null-type
    (sequential? schema) (array-type field-name schema)
    (map?* schema) (map-type schema)
    (both? schema) (both-type field-name schema)
    (enum? schema) (enum-type field-name schema)
    (record?* schema) (record-type field-name schema)
    :else (throw (RuntimeException. (str "Could not tranform field "
                                         field-name
                                         " with schema "
                                         schema)))))

(defn avro-field
  [field-name schema]
  (let [field-name* (-> (if (s/optional-key? field-name)
                          (:k field-name)
                          field-name)
                        name
                        ->snake_case)
        type (avro-type field-name* schema)]
    {:type type
     :name field-name*}))

(defn to-avro
  [prismatic-schema & {:keys [name namespace]}]
  (-> (avro-type "" prismatic-schema)
      (assoc :namespace (or namespace (-> (meta prismatic-schema)
                                          :ns
                                          str))
             :name (->PascalCase
                     (or name
                         (s/schema-name prismatic-schema)
                         (str "Schema" (gensym)))))
      (generate-string {:pretty true})))