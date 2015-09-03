(ns com.intentmedia.schema-transform.avro-transform
  (:require [cheshire.core :refer [parse-string generate-string]]
    [schema.core :as s]))

; Currently supports:
; - Primitives
; - Records
; - Enums
; - Arrays
;
; Not supported:
; - Maps
; - Fixed
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

(defn- avro-nullable->prismatic-nullable [type-field]
  (let [primitive (first (remove #(= "null" %) type-field))]
    (if (some #(= "null" %) type-field)
      (s/maybe (avro-primitive->prismatic-primitive primitive)))))

(defn avro-record-transformer [avro]
  (let [fields (get avro "fields")]
    (emit-pair avro (reduce (fn [combiner [k v]]
                              (assoc combiner k v))
                      {}
                      (map avro->prismatic-pair fields)))))

(defn avro-primitive-transformer [avro]
  (let [type-field (get avro "type")]
    (if (avro-primitive->prismatic-primitive type-field)
      (emit-pair avro (avro-primitive->prismatic-primitive type-field))
      (emit-pair avro (avro-nullable->prismatic-nullable type-field)))))

(defn avro-array-transformer [avro]
  (emit-pair avro [(avro-primitive->prismatic-primitive (get avro "items"))]))

(defn avro-enum-transformer [avro]
  (emit-pair avro (apply s/enum (get avro "symbols"))))

(def avro-type->transformer
  {"record"  avro-record-transformer
   "boolean" avro-primitive-transformer
   "int"     avro-primitive-transformer
   "long"    avro-primitive-transformer
   "float"   avro-primitive-transformer
   "double"  avro-primitive-transformer
   "string"  avro-primitive-transformer
   "array"   avro-array-transformer
   "enum"    avro-enum-transformer})

(defn avro->prismatic-pair [avro]
  (let [raw-type (get avro "type")
        type (if (contains? avro-type->transformer raw-type)
               raw-type
               (first (remove #(= "null" %) raw-type)))
        transformer (get avro-type->transformer type)]
    (transformer avro)))

(defn avro-string->prismatic [avro-json-schema]
  (let [pair (avro->prismatic-pair (parse-string avro-json-schema))]
    (last pair)))
