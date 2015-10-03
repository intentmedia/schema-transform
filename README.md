# schema-transform
[![Build Status](https://travis-ci.org/intentmedia/schema-transform.svg)](https://travis-ci.org/intentmedia/schema-transform)

Transform data schemas

## Usage

###Schema -> Avro
```clj
(require '[schema.core :as s]
         '[com.intentmedia.schema-transform.prismatic-transform :refer [prismatic->avro]])

(s/defschema User
  {:name            String
   :favorite_number (s/maybe Integer)
   :favorite_color  (s/maybe String)})
   
(prismatic->avro example-schema :namespace "example.avro")
=> "{
      \"name\": \"User\",
      \"type\": \"record\",
      \"fields\": [
        {\"name\": \"name\", \"type\": \"string\"},
        {\"name\": \"favorite_number\", \"type\": [\"null\",\"int\"]},
        {\"name\": \"favorite_color\", \"type\": [\"null\",\"string\"]}
      ],
      \"namespace\": \"example.avro\"
    }"
```

###Avro -> Schema
```clj
(require '[schema.core :as s]
         '[com.intentmedia.schema-transform.avro-transform :refer [avro->prismatic]])

(def avro-schema "{\"name\": \"User\",
                   \"type\": \"record\",
                   \"fields\": [
                     {\"name\": \"name\", \"type\": \"string\"},
                     {\"name\": \"favorite_number\", \"type\": [\"null\",\"int\"]},
                     {\"name\": \"favorite_color\", \"type\": [\"null\",\"string\"]}
                   ],
                   \"namespace\": \"example.avro\"
                 }")

(avro->prismatic avro-schema)
=> {:name            String
    :favorite_number (s/maybe Integer)
    :favorite_color  (s/maybe String)}

                    
```

## Contributers

* Chet Mancini | (Github)[http://github.com/chetmancini], (Twitter)[http://twitter.com/chetmancini]
* Sebastian Rojas Vivanco | (Github)[https://github.com/sebastiansen]