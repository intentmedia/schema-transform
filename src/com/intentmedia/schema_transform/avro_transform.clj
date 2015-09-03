(ns com.intentmedia.schema-transform.avro-transform
  (:require [cheshire.core :refer [parse-string generate-string]]
    [schema.core :as s]))

; Currently supports:
; - Primitives
; - Records
; - Enums
; - Arrays
; - Unions
; - Null
;
; Not supported from Avro spec:
; - Maps
; - Fixed
; - Bytes

(declare avro->prismatic-pair)

(def avro-primitive->prismatic-primitive
  {"boolean" Boolean
   "int"     Integer
   "long"    Long
   "float"   Float
   "double"  Double
   "string"  String})

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
  nil)

(def avro-type->transformer
  {"record"  avro-record-transformer
   "boolean" avro-primitive-transformer
   "int"     avro-primitive-transformer
   "long"    avro-primitive-transformer
   "float"   avro-primitive-transformer
   "double"  avro-primitive-transformer
   "string"  avro-primitive-transformer
   "array"   avro-array-transformer
   "enum"    avro-enum-transformer
   "null"    avro-null-transformer})

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
