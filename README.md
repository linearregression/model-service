# Model Service [![Status](https://circleci.com/gh/kiip/model-service.png?circle-token=aa1f134faf12b56ad296a4bf50be37ee056bce1c)](https://circleci.com/gh/kiip/model-service)

Store and serve models trained from [fitbox](http://github.com/cdgore/fitbox)


Quick Start
-----------

Launch with sbt.


    sbt run


or build a jar with


    sbt assembly


Load a feature manager with

    curl -H 'Accept: application/json' -X PUT -d '
    {
      "k": 21,
      "label": "target",
      "numeric_label": "cpa",
      "single_features": [
        "application_id",
        "campaign_id",
        "connection_type",
        "ad_group",
        "location_country",
        "location_region",
        "location_city",
        "impression_slot"
      ],
      "quadratic_features": [
        [
          [
            "application_id"
          ],
          [
            "campaign_id",
            "ad_group",
            "connection_type",
            "location_country"
          ]
        ],
        [
          [
            "campaign_id"
          ],
          [
            "location_country",
            "impression_slot"
          ]
        ]
      ]
    }
    ' http://0.0.0.0:8080/models/model_1


This creates a namespace for model parameters to be loaded and used for predictions


    {"model_namespace": "model_1", "created_at": <DateTime>}

To view loaded models:

    curl -H 'Accept: application/json' -X GET http://0.0.0.0:8080/models

This should return

    {
        "model_1": {
            "created_at": <DateTime>,
            "modified_at": <DateTime>,
            "last_added": "parameters_1",
            "parameters": [
                "parameters_1"
            ]
        }
    }

Models consist of a feature manager and a set of parameters, where the feature manager
specifies the size of the parameter vector in terms of 2^k, the label to expect, 
a numeric label (optional), a list of single features, and a list of quadratic features, 
which are automatically expanded (ie. "application_id_x_campaign_id").

Parameter sets are specified in terms of their indices, data, and length. For this example,
to load a parameter set into the model:


    curl -H 'Accept: application/json' -X PUT -d '
    {
      "index": [0, 5, 100],
      "data": [-2.535, -1.884e-06, 4.547e-06],
      "length": 4194304
    }
    ' http://0.0.0.0:8080/models/model_1/parameters_1

This should return

    Parameters stored with key: parameters_1

Predictions are optimized for feature sets with nested trees.  For example, the feature set

    {
      "color": ["red", "blue"],
      "shape": ["round", "square"],
      "publisher_id": {"abc123": { ... } ... }
    }

will recursively be flattened, with a cross product performed at each level of the tree:

    [
      ({"color": "red", "shape": "round", "publisher_id", "abc123"}, { ... }],
      ({"color": "red", "shape": "square", "publisher_id", "abc123"}, { ... }],
      ({"color": "blue", "shape": "round", "publisher_id", "abc123"}, { ... }],
      ({"color": "blue", "shape": "square", "publisher_id", "abc123"}, { ... }]
    ]

and a prediction will be returned for each expanded row. Predictions can be made by POST
request to the ```/predict/<model_key>/<parameter_key>``` interface.  If ```<model_key>``` or 
```<parameter_key>``` are not specified, the most recently used key(s) will be used.

This chart shows the high level internal structure of the model service for a single node

<img src="/../static_resources/docs/images/model_service_chart.png?raw=true" width="650" height="854">
