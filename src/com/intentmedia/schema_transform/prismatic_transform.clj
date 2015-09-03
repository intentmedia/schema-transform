(ns com.intentmedia.schema-transform.prismatic-transform
  (:require [cheshire.core :refer [parse-string generate-string]]
    [schema.core :as s]))

(def prismatic-primitive->avro-primitive
  {Boolean "boolean"
   Integer "int"
   Long    "long"
   Float   "float"
   Double  "double"
   String  "string"})

(defn- prismatic-pair->avro-map [[k v]]
  (let [parse-maybe #(prismatic-primitive->avro-primitive (:schema %))
        value (if (contains? prismatic-primitive->avro-primitive v)
                (prismatic-primitive->avro-primitive v)
                [(parse-maybe v) "null"])]
    {:name k :type value}))

(defn prismatic->avro [prismatic-schema namespace name]
  (generate-string {:namespace namespace
                    :type      "record"
                    :name      name
                    :fields    (vec (map prismatic-pair->avro-map prismatic-schema))}))