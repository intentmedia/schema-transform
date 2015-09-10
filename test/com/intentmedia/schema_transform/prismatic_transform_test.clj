(ns com.intentmedia.schema-transform.prismatic-transform-test
  (:require [com.intentmedia.schema-transform.prismatic-transform :refer :all]
    [clojure.test :refer :all]
    [schema.core :as s]))

(def avro-nullable "{\"namespace\":\"example.avro\",\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"null\",\"int\"]},{\"name\":\"favorite_color\",\"type\":[\"null\",\"string\"]}]}")

(def prismatic-nullable
  {:name            String
   :favorite_number (s/maybe Integer)
   :favorite_color  (s/maybe String)})

(deftest test-prismatic->avro
  (testing "It correctly parses a prismatic schema into an avro schema"
    (is (= avro-nullable (prismatic->avro prismatic-nullable "example.avro" "User")))))


(deftest test-prismatic-primitive-transformer
  (testing "Converts a single field"
    (is (= {:name "field" :type "string"} (prismatic-primitive-transformer [:field String])))
    (is (= {:name "field" :type "int"} (prismatic-primitive-transformer [:field Integer])))
    (is (= {:name "field" :type "double"} (prismatic-primitive-transformer [:field Double])))
    (is (= {:name "field" :type "long"} (prismatic-primitive-transformer [:field Long])))
    (is (= {:name "field" :type "boolean"} (prismatic-primitive-transformer [:field Boolean])))
    (is (= {:name "field" :type "float"} (prismatic-primitive-transformer [:field Float]))))
  (testing "Converts a nullable field"
    (is (= {:name "field" :type ["null" "string"]} (prismatic-primitive-transformer [:field (s/maybe String)])))
    (is (= {:name "field" :type ["null" "int"]} (prismatic-primitive-transformer [:field (s/maybe Integer)])))
    (is (= {:name "field" :type ["null" "double"]} (prismatic-primitive-transformer [:field (s/maybe Double)])))
    (is (= {:name "field" :type ["null" "long"]} (prismatic-primitive-transformer [:field (s/maybe Long)])))
    (is (= {:name "field" :type ["null" "boolean"]} (prismatic-primitive-transformer [:field (s/maybe Boolean)])))
    (is (= {:name "field" :type ["null" "float"]} (prismatic-primitive-transformer [:field (s/maybe Float)])))))

(deftest test-prismatic-enum-transformer
  (testing "Converts an enum"
    (is (= {:name    "suits"
            :type    "enum"
            :symbols ["CLUBS" "DIAMONDS" "HEARTS" "SPADES"]}
          (prismatic-enum-transformer [:suits (s/enum "SPADES" "CLUBS" "DIAMONDS" "HEARTS")])))))

(deftest test-prismatic-record-transformer
  (testing "Converts a record"))

(deftest test-prismatic-array-transformer
  (testing "Converts an array"
    (is (= {:name  "arr"
            :type  "array"
            :items "string"}
          (prismatic-array-transformer [:arr [String]])))))

(deftest test-prismatic-map-transformer
  (testing "Converts a map"
    (is (= {:name   "map-name"
            :type   "map"
            :values "double"}
          (prismatic-map-transformer [:map-name {Integer Double}])))))

(deftest test-prismatic-null-transformer
  (testing "Converts a null"
    (is (= {:name "empty" :type "null"} (prismatic-null-transformer [:empty nil])))))