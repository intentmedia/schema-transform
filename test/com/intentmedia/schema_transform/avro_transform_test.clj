(ns com.intentmedia.schema-transform.avro-transform-test
  (:require [com.intentmedia.schema-transform.avro-transform :refer :all]
            [clojure.test :refer :all]
            [schema.core :as s]))

(def avro-nullable "{\"namespace\":\"example.avro\",\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"int\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}")

(def prismatic-nullable
  {:name            String
   :favorite_number (s/maybe Integer)
   :favorite_color  (s/maybe String)})

(deftest test-avro-primitive-transformer
  (testing "Converts a single field"
    (is (= [:name String] (avro-primitive-transformer {"name" "name" "type" "string"})))
    (is (= [:name Integer] (avro-primitive-transformer {"name" "name" "type" "int"})))
    (is (= [:name Double] (avro-primitive-transformer {"name" "name" "type" "double"})))
    (is (= [:name Long] (avro-primitive-transformer {"name" "name" "type" "long"})))
    (is (= [:name Boolean] (avro-primitive-transformer {"name" "name" "type" "boolean"})))
    (is (= [:name String] (avro-primitive-transformer {"name" "name" "type" "fixed"})))
    (is (= [:name String] (avro-primitive-transformer {"name" "name" "type" "bytes"}))))
  (testing "Converts a union (nullable) field in either order"
    (is (= [:name (s/maybe Integer)] (avro-primitive-transformer {"name" "name" "type" ["int" "null"]})))
    (is (= [:name (s/maybe Integer)] (avro-primitive-transformer {"name" "name" "type" ["null" "int"]})))))

(deftest test-avro-enum-transformer
  (testing "Converts an enum"
    (is (= [:suits (s/enum "SPADES" "HEARTS" "DIAMONDS" "CLUBS")]
           (avro-enum-transformer {"name"    "suits"
                                   "type"    "enum"
                                   "symbols" ["SPADES" "HEARTS" "DIAMONDS" "CLUBS"]})))))

(deftest test-avro-record-transformer
  (testing "Converts a record"
    (is (= [:rec {:name            String
                  :favorite_number (s/maybe Integer)}]
           (avro-record-transformer {"name"      "rec"
                                     "namespace" "example.avro"
                                     "type"      "record"
                                     "fields"    [{"name" "name" "type" "string"}
                                                  {"name" "favorite_number" "type" ["null" "int"]}]})))))

(deftest test-avro-array-transformer
  (testing "Converts an array"
    (is (= [:arr [String]] (avro-array-transformer {"name"  "arr"
                                                    "type"  "array"
                                                    "items" "string"})))))

(deftest test-avro-map-transformer
  (testing "Converts a map"
    (is (= [:map-name {String Double}] (avro-map-transformer {"name"   "map-name"
                                                              "type"   "map"
                                                              "values" "double"})))))

(deftest test-avro-null-transformer
  (testing "Converts null to nothing"
    (is (= [:empty nil] (avro-null-transformer {"name" "empty" "type" "null"})))))

(deftest test-avro->prismatic
  (testing "It correctly parses an avro schema into a Prismatic schema"
    (is (= prismatic-nullable (avro-string->prismatic avro-nullable)))))