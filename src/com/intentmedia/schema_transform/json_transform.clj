(ns com.intentmedia.schema-transform.json-transform
  (:require [cheshire.core :refer [parse-string]]
            [schema.core :as s]))

(declare json-type-transformer)


(def json-primitive->prismatic-primitive
  {"boolean" s/Bool
   "integer" s/Int
   "number"  s/Num
   "string"  s/Str
   "null"    nil})


(defn predicates
  ([min max] (predicates min max nil))
  ([min max unique]
   (cond-> []
     (and min max)
     (conj (s/pred #(<= min (count %) max) (format "(<= %d size %d)" min max)))

     min
     (conj (s/pred #(<= min (count %)) (format "(<= %d size)" min)))

     max
     (conj (s/pred #(<= (count %) max) (format "(<= size %d)" max)))

     unique
     (conj (s/pred #(= % (distinct %)) "unique")))))


(defn add-preds [schema preds]
  (if (empty? preds)
    schema
    (apply s/both (cons schema preds))))


(defn json-object-props-transformer [json-object-type]
  (let [properties (:properties json-object-type)
        required   (->> json-object-type :required (map keyword) (into #{}))
        required?  (partial contains? required)]
    (->> properties
      (map
        (fn [[name schema]]
          (let [key-modifier (if (required? name)
                               identity
                               s/optional-key)]
            [(key-modifier name)
             (json-type-transformer schema)])))
      (reduce
        (fn [combiner [k v]]
          (assoc combiner k v))
        {}))))


(defn json-object-additional-props-transformer [transformed add-props]
  (cond
    (false? add-props)
    transformed

    (map? add-props)
    (assoc transformed s/Str (json-type-transformer add-props))

    :else
    (assoc transformed s/Str s/Any)))


(defn json-object-transformer [json-object-type]
  (let [add-props (:additionalProperties json-object-type)
        preds (predicates (:minProperties json-object-type) (:maxProperties json-object-type))]
    (-> (json-object-props-transformer json-object-type)
      (json-object-additional-props-transformer add-props)
      (add-preds preds))))


(defn json-tuple-transformer [json-array-type]
  (let [schema (->> (:items json-array-type)
                 (map-indexed #(s/optional (json-type-transformer %2) (str (inc %1))))
                 (into []))]
    (if (false? (:additionalItems json-array-type))
      schema
      (conj schema s/Any))))


(defn json-list-transformer [json-array-type]
  [(json-type-transformer (:items json-array-type))])


(defn json-array-transformer [json-array-type]
  (let [preds (predicates
                (:minItems json-array-type)
                (:maxItems json-array-type)
                (:uniqueItems json-array-type))]
    (-> (if (map? (:items json-array-type))
          (json-list-transformer json-array-type)
          (json-tuple-transformer json-array-type))
      (add-preds preds))))


(defn json-nil? [type]
  (= "null" type))

(defn enum? [json-type]
  (vector? (:enum json-type)))

(defn nilable? [types]
  (some json-nil? types))

(defn union? [types]
  (and (vector? types)
       (> (count types) 0)))


(defn json-enum-transformer [json-enum-type]
  (apply s/enum (:enum json-enum-type)))


(defn json-nilable-transformer [json-nilable-type]
  (let [types (into [] (remove json-nil? (:type json-nilable-type)))]
    (s/maybe (json-type-transformer
               (assoc json-nilable-type :type types)))))


(defn json-union-type-transformer [json-union-type]
  (let [types (:type json-union-type)]
    (if (= 1 (count types))
      (json-primitive->prismatic-primitive (first types))
      (apply s/cond-pre (map json-primitive->prismatic-primitive types)))))


(def json-type->transformer
  {"object" json-object-transformer
   "array"  json-array-transformer})


(defn json-type-transformer [json-type]
  (let [type (:type json-type)]
    (cond
      (nilable? type)
      (json-nilable-transformer json-type)

      (enum? json-type)
      (json-enum-transformer json-type)

      (union? type)
      (json-union-type-transformer json-type)

      (contains? json-primitive->prismatic-primitive type)
      (json-primitive->prismatic-primitive type)

      :else
      (let [transformer (json-type->transformer type)]
        (if transformer
          (transformer json-type)
          (throw (ex-info (str "No transformer for type " type) {:json-type json-type})))))))


(defn json-parsed->prismatic [json]
  (json-type-transformer json))


(defn json->prismatic [json]
  (json-parsed->prismatic (parse-string json true)))
