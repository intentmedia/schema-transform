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

(defn is-optional-field? [prismatic-type]
  (contains? prismatic-type :schema))

(defn get-name [prismatic-schema]
  (name (first prismatic-schema)))

(declare prismatic-pair->avro)

(defn prismatic-enum-transformer [prismatic-schema]
  (let [v (last prismatic-schema)
        symbols (vec (sort (rest (s/explain v))))]
    {:name    (get-name prismatic-schema)
     :type    "enum"
     :symbols symbols}))

(defn prismatic-array-transformer [prismatic-schema]
  {:name  (get-name prismatic-schema)
   :type  "array"
   :items (prismatic-primitive->avro-primitive (first (last prismatic-schema)))})

(defn prismatic-map-transformer [prismatic-schema]
  {:name   (get-name prismatic-schema)
   :type   "map"
   :values (prismatic-primitive->avro-primitive (last (first (last prismatic-schema))))})

(defn prismatic-null-transformer [prismatic-schema]
  {:name (get-name prismatic-schema) :type "null"})

(defn prismatic-primitive-transformer [prismatic-schema]
  (let [v (last prismatic-schema)
        value (if (contains? prismatic-primitive->avro-primitive v)
                (prismatic-primitive->avro-primitive v)
                (make-union v))]
    {:name (get-name prismatic-schema) :type value}))

(defn prismatic-record-transformer [prismatic-schema]
  (let [name (get-name prismatic-schema)
        fields (map prismatic-pair->avro (last prismatic-schema))]
    {:name   name
     :type   "record"
     :fields fields}))

(defn prismatic-pair->avro [prismatic-schema]
  (let [value-type (last prismatic-schema)
        is-primitive? #(or (contains? prismatic-primitive->avro-primitive %) (is-optional-field? %))
        is-array? #(seq? %)
        is-enum? #(= schema.core.EnumSchema (class %))
        is-record? #(and (map? %) (keyword? (first (first %))))
        is-map? #(and (map? %) (= (count %) 1))
        is-null? #(nil? %)]
    (cond
      (is-primitive? value-type) (prismatic-primitive-transformer prismatic-schema)
      (is-enum? value-type) (prismatic-enum-transformer prismatic-schema)
      (is-array? value-type) (prismatic-array-transformer prismatic-schema)
      (is-map? value-type) (prismatic-map-transformer prismatic-schema)
      (is-record? value-type) (prismatic-record-transformer prismatic-schema)
      (is-null? value-type) (prismatic-null-transformer prismatic-schema))))


(defn prismatic->avro [prismatic-schema namespace name]
  (let [pair [(keyword name) prismatic-schema]
        avro (prismatic-pair->avro pair)
        output (if namespace (assoc avro :namespace namespace) avro)]
    (generate-string output)))

