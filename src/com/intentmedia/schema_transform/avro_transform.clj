(ns com.intentmedia.schema-transform.avro-transform
  (:require [cheshire.core :refer [parse-string generate-string]]
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
;
; Not supported from Avro spec:
; - Collections of records

(declare avro->prismatic-pair)

(def avro-primitive->prismatic-primitive
  {"boolean" Boolean
   "int"     Integer
   "long"    Long
   "float"   Float
   "double"  Double
   "string"  String
   "bytes"   String
   "fixed"   String})

(defn emit-pair [avro value]
  (let [key (keyword (get avro "name"))]
    [key value]))

(defn is-union? [avro]
  (some #(= "null" %) avro))

(defn avro-union->type-str [union]
  (first (remove #(= "null" %) union)))

(defn- avro-nullable->prismatic-nullable [type-field]
  (let [primitive (avro-union->type-str type-field)]
    (if (is-union? type-field)
      (s/maybe (avro-primitive->prismatic-primitive primitive)))))

(defn avro-record-transformer [avro]
  (let [fields (get avro "fields")]
    (emit-pair avro (reduce (fn [combiner [k v]]
                              (assoc combiner k v))
                      {}
                      (map avro->prismatic-pair fields)))))

(defn avro-primitive-transformer [avro]
  (let [type-field (get avro "type")]
    (if (is-union? type-field)
      (emit-pair avro (avro-nullable->prismatic-nullable type-field))
      (emit-pair avro (avro-primitive->prismatic-primitive type-field)))))

(defn avro-array-transformer [avro]
  (emit-pair avro [(avro-primitive->prismatic-primitive (get avro "items"))]))

(defn avro-enum-transformer [avro]
  (emit-pair avro (apply s/enum (get avro "symbols"))))

(defn avro-null-transformer [avro]
  "Avro supports null types. Prismatic does not really have this so we'll just return nil."
  nil)

(defn avro-map-transformer [avro]
  "Transform an avro map spec to Prismatic schema map description.
  While Prismatic supports different key types, the Avro spec assumes string keys so we hardcode that."
  (if-let [value-type (avro-primitive->prismatic-primitive (get avro "values"))]
    (emit-pair avro {String value-type})))

(def avro-type->transformer
  {"record"  avro-record-transformer
   "boolean" avro-primitive-transformer
   "int"     avro-primitive-transformer
   "long"    avro-primitive-transformer
   "float"   avro-primitive-transformer
   "double"  avro-primitive-transformer
   "string"  avro-primitive-transformer
   "bytes"   avro-primitive-transformer
   "fixed"   avro-primitive-transformer
   "array"   avro-array-transformer
   "enum"    avro-enum-transformer
   "null"    avro-null-transformer
   "map"     avro-map-transformer})

(defn avro->prismatic-pair [avro]
  (let [raw-type (get avro "type")
        type (cond
               (contains? avro-type->transformer raw-type) raw-type
               (is-union? raw-type) (avro-union->type-str raw-type))
        transformer (get avro-type->transformer type)]
    (transformer avro)))

(defn avro-string->prismatic [avro-json-schema]
  (let [pair (avro->prismatic-pair (parse-string avro-json-schema))]
    (last pair)))
