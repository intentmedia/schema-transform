(ns com.intentmedia.schema-transform.json-transform-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]

            [schema.core :as s]
            [cheshire.core :refer [generate-string]]
            [com.intentmedia.schema-transform.json-transform :refer :all]))


(defn- read-schema [filename]
  (slurp (io/file (io/resource filename))))


(deftest test-primitive-types
  (are [json prismatic] (= prismatic (json-primitive->prismatic-primitive json))
    "boolean" s/Bool
    "integer" s/Int
    "number"  s/Num
    "string"  s/Str
    "null"    nil))


(deftest test-object-additional-properties
  (are [json prismatic] (= prismatic (json->prismatic (generate-string json)))
    {:type "object"}
    {s/Str s/Any}

    {:type "object"
     :additionalProperties false}
    {}

    {:type "object"
     :additionalProperties {:type "boolean"}}
    {s/Str s/Bool}))


(deftest test-array-schemas
  (testing "test uniform list"
    (are [json prismatic] (= prismatic (json->prismatic (generate-string json)))
      {:type "array"}
      [s/Any]

      {:type "array"
       :items {:type "number"}}
      [s/Num]))

  (testing "test tuple"
    (are [json prismatic] (= prismatic (json->prismatic (generate-string json)))
      {:type "array"
       :items [{:type "integer"} {:type "string"}]}
      [(s/optional s/Int "1") (s/optional s/Str "2") s/Any]

      {:type "array"
       :additionalItems false
       :items [{:type "integer"} {:type "string" :enum ["one" "two" "zero"]}]}
      [(s/optional s/Int "1") (s/optional (s/enum "one" "two" "zero") "2")])))


(deftest test-json-transform
  (testing "Converts a simple object type"
    (is (= {:order_id s/Int
            :customer_id s/Int
            :total s/Num}
          (json->prismatic (read-schema "simple.json")))))

  (testing "Converts a complex object type"
    (is (= {:order_id s/Int
            :customer_id s/Int
            :total s/Num
            :order_details [{:quantity s/Int
                             :total s/Num
                             :product_detail {:product_id s/Int
                                              :product_name s/Str
                                              (s/optional-key :product_description) (s/maybe s/Str)
                                              :product_status (s/enum "AVAILABLE" "OUT_OF_STOCK")
                                              :product_tags [s/Str]
                                              :price s/Num
                                              :product_properties {s/Str s/Str}}}]}
          (json->prismatic (read-schema "complex.json"))))))
