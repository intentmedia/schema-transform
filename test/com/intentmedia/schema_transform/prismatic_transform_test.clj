(ns com.intentmedia.schema-transform.prismatic-transform-test
  (:require [com.intentmedia.schema-transform.prismatic-transform :refer :all]
    [clojure.test :refer :all]
    [schema.core :as s]))

(def avro-nullable "{\"namespace\":\"example.avro\",\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"int\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}")

(def prismatic-nullable
  {:name            String
   :favorite_number (s/maybe Integer)
   :favorite_color  (s/maybe String)})

(deftest test-prismatic->avro
  (testing "It correctly parses a prismatic schema into an avro schema"
    (is (= avro-nullable (prismatic->avro prismatic-nullable "example.avro" "User")))))