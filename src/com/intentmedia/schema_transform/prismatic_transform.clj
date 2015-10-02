(ns com.intentmedia.schema-transform.prismatic-transform
  (:require [cheshire.core :refer [parse-string generate-string]]
            [schema.core :as s]
            [camel-snake-kebab.core :refer [->PascalCase]])
  (:import [schema.core EnumSchema Maybe]))

(defrecord Optional [schema])

(def prismatic-primitive->avro-primitive
  {Boolean "boolean"
   Double  "double"
   Float   "float"
   Integer "int"
   Long    "long"
   Number  "double"
   String  "string"})

(declare prismatic->avro*)

(defn prismatic-enum-transformer [prismatic-schema]
  (let [symbols (vec (sort (rest (s/explain prismatic-schema))))]
    {:type    "enum"
     :symbols symbols}))

(defn prismatic-array-transformer [prismatic-schema]
  {:type  "array"
   :items (prismatic-primitive->avro-primitive (first prismatic-schema))})

(defn prismatic-map-transformer [prismatic-schema]
  {:type   "map"
   :values (prismatic-primitive->avro-primitive (first (vals prismatic-schema)))})

(defn prismatic-null-transformer [_] {:type "null"})

(defn prismatic-primitive-transformer [prismatic-schema]
  {:type (prismatic-primitive->avro-primitive prismatic-schema)})

(defn prismatic-record-transformer [field-name prismatic-schema]
  (let [fields (map prismatic->avro*
                    (keys prismatic-schema)
                    (vals prismatic-schema))]
    {:type   "record"
     :name   (or (s/schema-name prismatic-schema)
                 (->PascalCase (gensym (name field-name))))
     :fields fields}))

(defn primitive? [value-type]
  (contains? prismatic-primitive->avro-primitive value-type))

(defn enum? [value-type]
  (= EnumSchema (class value-type)))

(defn record?* [value-type]
  (and (map? value-type) (keyword? (first (first value-type)))))

(defn map?* [value-type]
  (and (map? value-type)
       (= (count value-type) 1)
       (= (first (keys value-type)) String)))

(defn prismatic-transform [value-type]
  ((cond
     (primitive? value-type) prismatic-primitive-transformer
     (enum? value-type) prismatic-enum-transformer
     (seq? value-type) prismatic-array-transformer
     (nil? value-type) prismatic-null-transformer
     (map?* value-type) prismatic-map-transformer)
    value-type))

(defn prismatic-optional-transform [field-name value-type]
  {:type ["null" (if (primitive? value-type)
                   (prismatic-primitive->avro-primitive value-type)
                   (prismatic->avro* field-name value-type))]
   :name (name field-name)})

(defn prismatic->avro* [field-name value-type]
  (let [optional? (or (s/optional-key? field-name)
                      (= (type value-type) Maybe))
        field-name (if (s/optional-key? field-name)
                     (:k field-name)
                     field-name)
        value-type (if (= (type value-type) Maybe)
                     (:schema value-type)
                     value-type)]
    (cond
      optional? (prismatic-optional-transform field-name value-type)
      (record?* value-type) (prismatic-record-transformer field-name value-type)
      :else (assoc (prismatic-transform value-type)
              :name (name field-name)))))

(defn prismatic->avro
  [prismatic-schema & {:keys [name namespace]}]
  (cond-> (prismatic->avro* (or name (->PascalCase (str (gensym))))
                            prismatic-schema)
          namespace (assoc :namespace namespace)
          true (generate-string {:pretty true})))