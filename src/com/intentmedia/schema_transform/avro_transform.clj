(ns com.intentmedia.schema-transform.avro-transform
  (:require [cheshire.core :refer [parse-string]]
            [schema.core :as s]))

; Currently supports:
; - Primitives
; - Records
; - Enums
; - Arrays of primitives
; - Maps of primitives
; - Unions
; - Null
; - Fixed (as strings)
; - Bytes (as strings)

(declare avro-pair->prismatic-pair)
(declare avro-type-transformer)

(def avro-primitive->prismatic-primitive
  {"boolean" Boolean
   "int"     Integer
   "long"    Long
   "float"   Float
   "double"  Double
   "string"  String
   "bytes"   String
   "fixed"   String
   "null"    nil})

(defn is-nullable? [avro]
  (some #(= "null" %) avro))

(defn avro-union->type-str [union]
  (first (remove #(= "null" %) union)))

(defn- avro-nullable->prismatic-nullable [union-field]
  (let [primitive (avro-union->type-str union-field)]
    (if (is-nullable? union-field)
      (s/maybe (avro-primitive->prismatic-primitive primitive)))))

(defn avro-primitive-transformer [union-or-primitive]
  (if (is-nullable? union-or-primitive)
    (avro-nullable->prismatic-nullable union-or-primitive)
    (avro-primitive->prismatic-primitive union-or-primitive)))

(defn avro-record-transformer [avro-record-type]
  (let [fields (get avro-record-type :fields)]
    (reduce (fn [combiner [k v]]
              (assoc combiner k v))
      {}
      (map avro-pair->prismatic-pair fields))))

(defn avro-array-transformer [avro-array-type]
  (let [item-raw-type (get avro-array-type :items)]
    [(avro-type-transformer item-raw-type)]))

(defn avro-enum-transformer [avro-enum-type]
  (apply s/enum (get avro-enum-type :symbols)))

(defn avro-map-transformer [avro-map-type]
  (if-let [value-type (avro-primitive->prismatic-primitive (get avro-map-type :values))]
    {String value-type}))

(defn avro-fixed-transformer [avro-fixed-type]
  String)

(defn is-union? [avro-type]
  (vector? avro-type))

(defn avro-union-transformer [avro-type]
  (apply s/cond-pre
    (map avro-type-transformer avro-type)))

(def avro-type->transformer
  {"record" avro-record-transformer
   "array"  avro-array-transformer
   "enum"   avro-enum-transformer
   "map"    avro-map-transformer
   "fixed"  avro-fixed-transformer})

(defn avro-type-transformer [avro-type]
  (cond
    (contains? avro-primitive->prismatic-primitive avro-type)
    (avro-primitive-transformer avro-type)

    (is-nullable? avro-type)
    (avro-primitive-transformer avro-type)

    (is-union? avro-type)
    (avro-union-transformer avro-type)

    :else
    ((-> avro-type :type avro-type->transformer) avro-type)))

(defn avro-pair->prismatic-pair [avro-pair-map]
  (let [name (get avro-pair-map :name)
        value-type (get avro-pair-map :type)]
    [(keyword name) (avro-type-transformer value-type)]))

(defn avro-parsed->prismatic [avro]
  (avro-type-transformer avro))

(defn avro->prismatic [avro]
  (avro-parsed->prismatic (parse-string avro true)))
