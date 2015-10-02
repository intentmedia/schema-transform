(ns com.intentmedia.schema-transform.prismatic-transform-test
  (:require [com.intentmedia.schema-transform.prismatic-transform :refer :all]
            [clojure.test :refer :all]
            [schema.core :as s]))

(def avro-nullable "{\n  \"type\" : \"record\",\n  \"name\" : \"User\",\n  \"fields\" : [ {\n    \"type\" : \"string\",\n    \"name\" : \"name\"\n  }, {\n    \"type\" : [ \"null\", \"int\" ],\n    \"name\" : \"favorite_number\"\n  }, {\n    \"type\" : [ \"null\", \"string\" ],\n    \"name\" : \"favorite_color\"\n  }, {\n    \"type\" : \"map\",\n    \"values\" : \"long\",\n    \"name\" : \"map\"\n  }, {\n    \"type\" : [ \"null\", {\n      \"type\" : \"record\",\n      \"name\" : \"NestedRecord\",\n      \"fields\" : [ {\n        \"type\" : \"boolean\",\n        \"name\" : \"nested_field\"\n      } ]\n    } ],\n    \"name\" : \"nested\"\n  } ],\n  \"namespace\" : \"example.avro\"\n}")

(s/defschema User
  {:name                    String
   :favorite_number         (s/maybe Integer)
   :favorite_color          (s/maybe String)
   :map                     {String Long}
   (s/optional-key :nested) (s/schema-with-name {:nested_field Boolean}
                                                "NestedRecord")})

(deftest test-prismatic->avro
  (testing "It correctly parses a prismatic schema into an avro schema"
    (is (= avro-nullable (prismatic->avro User :namespace "example.avro")))))

(deftest test-prismatic-primitive-transformer
  (testing "Converts a single field"
    (is (= {:type "string"} (prismatic-primitive-transformer String)))
    (is (= {:type "int"} (prismatic-primitive-transformer Integer)))
    (is (= {:type "double"} (prismatic-primitive-transformer Double)))
    (is (= {:type "long"} (prismatic-primitive-transformer Long)))
    (is (= {:type "boolean"} (prismatic-primitive-transformer Boolean)))
    (is (= {:type "float"} (prismatic-primitive-transformer Float)))))

(deftest test-prismatic-enum-transforTmer
  (testing "Converts an enum"
    (is (= {:type    "enum"
            :symbols ["CLUBS" "DIAMONDS" "HEARTS" "SPADES"]}
           (prismatic-enum-transformer (s/enum "SPADES" "CLUBS" "DIAMONDS" "HEARTS"))))))

(deftest test-prismatic-record-transformer
  (testing "Converts a record"
    (is (= {:type   "record"
            :fields [{:name "name" :type "string"}
                     {:name "favorite_number" :type ["null" "int"]}]}
           (select-keys
             (prismatic-record-transformer
              "Record"
              {:name            String
               :favorite_number (s/maybe Integer)})
             [:type :fields])))))

(deftest test-prismatic-array-transformer
  (testing "Converts an array"
    (is (= {:type  "array"
            :items "string"}
           (prismatic-array-transformer [String])))))

(deftest test-prismatic-map-transformer
  (testing "Converts a map"
    (is (= {:type   "map"
            :values "double"}
           (prismatic-map-transformer {Integer Double})))))

(deftest test-prismatic-null-transformer
  (testing "Converts a null"
    (is (= {:type "null"} (prismatic-null-transformer nil)))))