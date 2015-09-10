(ns com.intentmedia.schema-transform.prismatic-transform
  (:require [cheshire.core :refer [parse-string generate-string]]
    [schema.core :as s]))

(def prismatic-primitive->avro-primitive
  {Boolean "boolean"
   Double  "double"
   Float   "float"
   Integer "int"
   Long    "long"
   Number  "double"
   String  "string"})

(def prismatic-string-prefixes
  {"\\u" "bytes"})

(defn make-union [prismatic-type]
  ["null" (prismatic-primitive->avro-primitive (:schema prismatic-type))])

(defn- prismatic-pair->avro-map [[k v]]
  (let [value (if (contains? prismatic-primitive->avro-primitive v)
                (prismatic-primitive->avro-primitive v)
                (make-union v))]
    {:name k :type value}))


(defn prismatic-enum-transformer [prismatic-schema]
  (let [k (name (first prismatic-schema))
        v (last prismatic-schema)
        symbols (vec (sort (rest (s/explain v)))) ]
    {:name k
     :type "enum"
     :symbols symbols}))

(defn prismatic-array-transformer [prismatic-schema]
  {})

(defn prismatic-map-transformer [prismatic-schema]
  {})

(defn prismatic-null-transformer [prismatic-schema]
  (let [k (name (first prismatic-schema))]
    {:name k :type "null"}))

(defn prismatic-primitive-transformer [prismatic-schema]
  (let [k (name (first prismatic-schema))
        v (last prismatic-schema)
        value (if (contains? prismatic-primitive->avro-primitive v)
                (prismatic-primitive->avro-primitive v)
                (make-union v))]
    {:name k :type value}))

(defn prismatic-record-transformer [prismatic-schema]
  {})


(defn prismatic->avro [prismatic-schema namespace name]
  (generate-string {:namespace namespace
                    :type      "record"
                    :name      name
                    :fields    (vec (map prismatic-pair->avro-map prismatic-schema))}))

