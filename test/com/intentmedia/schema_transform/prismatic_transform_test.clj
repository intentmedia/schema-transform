(ns com.intentmedia.schema-transform.prismatic-transform-test
  (:require [com.intentmedia.schema-transform.prismatic-transform :refer :all]
            [cheshire.core :refer [generate-string]]
            [clojure.test :refer :all]
            [schema.core :as s])
  (:import [org.apache.avro Schema$Parser Schema$UnionSchema]))

(def avro-nullable [{:type      "enum",
                     :symbols   ["VAL2" "VAL1"],
                     :name      "Enum",
                     :namespace "com.intentmedia.schema-transform.prismatic-transform-test"}
                    {:type      "record",
                     :fields    [{:type "string", :name "field"}],
                     :name      "Nested2",
                     :namespace "com.intentmedia.schema-transform.prismatic-transform-test"}
                    {:type      "record",
                     :fields    [{:type "int", :name "thing"} {:type "Nested2", :name "nested_2"}],
                     :name      "AnonRecordsItem",
                     :namespace "com.intentmedia.schema-transform.prismatic-transform-test"}
                    {:type      "record",
                     :fields    [{:type "string", :name "field"}],
                     :name      "Other",
                     :namespace "com.intentmedia.schema-transform.prismatic-transform-test"}
                    {:type      "record",
                     :fields    [{:type "int", :name "thing"}],
                     :name      "AnonRecord",
                     :namespace "com.intentmedia.schema-transform.prismatic-transform-test"}
                    {:type      "record",
                     :fields    [{:type "string", :name "field"}],
                     :name      "OptionalRecord",
                     :namespace "com.intentmedia.schema-transform.prismatic-transform-test"}
                    {:type      "record",
                     :fields    [{:type "boolean", :name "nested_field"}],
                     :name      "NestedRecord",
                     :namespace "com.intentmedia.schema-transform.prismatic-transform-test"}
                    {:type      "record",
                     :fields    [{:type "Enum", :name "enum"}
                                 {:type "string", :name "name"}
                                 {:type {:type "array", :items "string"}, :name "string_array"}
                                 {:type {:type "array", :items "AnonRecordsItem"}, :name "anon_records"}
                                 {:type {:type "array", :items "Other"}, :name "list"}
                                 {:type ["null" "string"], :name "favorite_color"}
                                 {:type "AnonRecord", :name "anon_record"}
                                 {:type ["null" "OptionalRecord"], :name "optional_record"}
                                 {:type "int", :name "both"}
                                 {:type ["null" "int"], :name "favorite_number"}
                                 {:type {:type "map", :values "long"}, :name "map"}
                                 {:type "NestedRecord", :name "nested"}],
                     :name      "User",
                     :namespace "com.intentmedia.schema-transform.prismatic-transform-test"}])

(s/defschema Other {:field String})

(s/defschema User
  {:name                    String
   :favorite-number         (s/maybe Integer)
   :favorite-color          (s/maybe String)
   :map                     {String Long}
   :list                    [Other]
   :string-array            [String]
   :both                    (s/both Integer (s/pred pos?))
   :anon-record             {:thing Integer}
   :enum                    (s/enum "VAL1" "VAL2")
   :optional-record         (s/maybe {:field String})
   :anon-records            [{:thing Integer :nested-2 {:field String}}]
   (s/optional-key :nested) (s/schema-with-name {:nested-field Boolean}
                                                "NestedRecord")})

(deftest test-prismatic->avro
  (testing "It correctly parses a prismatic schema into an avro schema"
    (is (= avro-nullable (to-avro User)))))

(deftest parse-test
  (is (instance? Schema$UnionSchema
                 (.parse (Schema$Parser.)
                         (generate-string (to-avro User))))))

(deftest test-prismatic-primitive-transformer
  (testing "Converts a single field"
    (is (= "string" (primitive-type String)))
    (is (= "int" (primitive-type Integer)))
    (is (= "double" (primitive-type Double)))
    (is (= "long" (primitive-type Long)))
    (is (= "boolean" (primitive-type Boolean)))
    (is (= "float" (primitive-type Float)))))