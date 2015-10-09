(ns com.intentmedia.schema-transform.prismatic-transform-test
  (:require [com.intentmedia.schema-transform.prismatic-transform :refer :all]
            [clojure.test :refer :all]
            [schema.core :as s]))

(def avro-nullable "{\n  \"type\" : \"record\",\n  \"name\" : \"User\",\n  \"fields\" : [ {\n    \"type\" : \"string\",\n    \"name\" : \"name\"\n  }, {\n    \"type\" : [ \"null\", \"int\" ],\n    \"name\" : \"favorite_number\"\n  }, {\n    \"type\" : [ \"null\", \"string\" ],\n    \"name\" : \"favorite_color\"\n  }, {\n    \"type\" : {\n      \"type\" : \"map\",\n      \"values\" : \"long\"\n    },\n    \"name\" : \"map\"\n  }, {\n    \"type\" : \"int\",\n    \"name\" : \"both\"\n  }, {\n    \"type\" : {\n      \"type\" : \"record\",\n      \"name\" : \"NestedRecord\",\n      \"fields\" : [ {\n        \"type\" : \"boolean\",\n        \"name\" : \"nested_field\"\n      } ]\n    },\n    \"name\" : \"nested\"\n  } ],\n  \"namespace\" : \"com.intentmedia.schema-transform.prismatic-transform-test\"\n}")

(s/defschema User
  {:name                    String
   :favorite_number         (s/maybe Integer)
   :favorite_color          (s/maybe String)
   :map                     {String Long}
   :both                    (s/both Integer (s/pred pos?))
   (s/optional-key :nested) (s/schema-with-name {:nested_field Boolean}
                                                "NestedRecord")})

(deftest test-prismatic->avro
  (testing "It correctly parses a prismatic schema into an avro schema"
    (is (= avro-nullable (to-avro User)))))

(deftest test-prismatic-primitive-transformer
  (testing "Converts a single field"
    (is (= "string" (primitive-type String)))
    (is (= "int" (primitive-type Integer)))
    (is (= "double" (primitive-type Double)))
    (is (= "long" (primitive-type Long)))
    (is (= "boolean" (primitive-type Boolean)))
    (is (= "float" (primitive-type Float)))))

(deftest test-prismatic-enum-transforTnmer
  (testing "Converts an enum"
    (is (= {:type    "enum"
            :symbols ["DIAMONDS" "SPADES" "HEARTS" "CLUBS"]}
           (select-keys
             (enum-type :any (s/enum "SPADES" "CLUBS" "DIAMONDS" "HEARTS"))
             [:type :symbols])))))

(deftest test-prismatic-record-transformer
  (testing "Converts a record"
    (is (= {:type   "record"
            :fields [{:name "name" :type "string"}
                     {:name "favorite_number" :type ["null" "int"]}]}
           (select-keys
             (record-type
               "Record"
               {:name            String
                :favorite_number (s/maybe Integer)})
             [:type :fields])))))

(deftest test-prismatic-array-transformer
  (testing "Converts an array"
    (is (= {:type  "array"
            :items "string"}
           (array-type :any [String])))))

(deftest test-prismatic-map-transformer
  (testing "Converts a map"
    (is (= {:type   "map"
            :values "double"}
           (map-type {Integer Double})))))