# KairosDB Persistor

This is a [Vertx module](http://vertx.io/) that allows data to be inserted, queryed and deleted from the [KairosDB time series database](https://code.google.com/p/kairosdb/).

The module is implemented on top of the [KairosDB REST interface](https://code.google.com/p/kairosdb/wiki/Overview)

## Configuration

The module takes the following configuration, as 

```
{
    "address": <address>,
    "host": <host>,
    "port": <port>
}
```
where the parameters are

* `address`: The main address for the module. Every module has a main address. Defaults to `jonnywray.kairospersistor`
* `host`: Host name or ip address of the KairosDB instance. Defaults to `localhost`
* `port`: Port at which the KairosDB instance is listening. Defaults to `8080`

## Operations

The module currently supports the following operations. 


### *Add data points*

This operation will add data points to the database. The command supports both single data point addition and multiple. 

To add a single data point send the following (example) message to the address of the module

```
{
  "action" : "add_data_points",
  "datapoints" : {
    "name" : "integration.tests",
    "timestamp" : 1386622880074,
    "value" : 42,
    "tags" : {
      "test_type" : "integration"
    }
  }
}

```
and to add multiple data points send the (example) message

```
{
  "action" : "add_data_points",
  "datapoints" : {
    "name" : "integration.tests",
    "datapoints" : [ [ 1386622973642, 42 ], [ 1386622973742, 52 ] ],
    "tags" : {
      "test_type" : "integration"
    }
  }
}
```

In both cases the inner `datapoints` object is the JSON expected by the [KairosDB REST interface](https://code.google.com/p/kairosdb/wiki/AddDataPoints) and further details of the required data can be found on the KairosDB wiki

If data insertion is successful the following message is returned

```
{
  "status" : "ok"
}
```

### *Delete data points*

This operation will delete data points from the database. A JSON object parameter `query` is a KairosDB query used to
specify what points are to be deleted.

```
{
  "action" : "delete_data_points",
  "query" : {
         <KairosDB query object>
  }
}
```

A successful request will return

```
{
  "status" : "ok"
}
```

### *Delete metric*

In order to delete a metric send the following message to the module address

```
{
    "action": "delete_metric",
    "metric_name": <metric name>
}
```
where `metric_name` is the metric to be deleted. A successful request will return


```
{
  "status" : "ok"
}
```

### *List metric names*

To obtain a list of all metric names send the following message to the module address

```
{
    "action": "list_metric_names"
}
```

and a successful request will return (for example);

```
{
  "results" : [ "integration.tests", "kairosdb.datastore.query_time", "kairosdb.protocol.telnet_request_count", "kairosdb.protocol.http_request_count", "kairosdb.jvm.thread_count", "kairosdb.jvm.total_memory", "kairosdb.jvm.max_memory", "kairosdb.http.request_time", "kairosdb.jvm.free_memory", "kairosdb.datastore.query_collisions", "kairosdb.http.query_time", "kairosdb.metric_counters" ],
  "status" : "ok"
}
```

### *List tag names*

To obtain a list of all tag names send the following message to the module address

```
{
    "action": "list_tag_names"
}
```
and a successful request with return, for example,


```
{
  "results" : [ "method", "location", "test_type", "host", "metric_name", "query_index", "source", "request" ],
  "status" : "ok"
}

```

### *List tag values*

To obtain a list of all tag values send the following message to the module address

```
{
    "action": "list_tag_values"
}
```
with a successful response looking like, for example,

```
{
  "results" : [ "integration.tests", "query", "fake.metric", "tagnames", "delete", "weather.humidity", "weather.temp", "put", "kairosdb.protocol.http_request_count", "kairosdb.jvm.total_memory", "integration", "metricnames", "version", "tags", "London", "New_York", "tagvalues", "" ],
  "status" : "ok"
}
```

### *Query Metrics*

This operation will return a list of metric values matching the query. The JSON object parameter `query` is a KairosDB query used to
specify what points are to be returned.

```
{
  "action" : "query_metrics",
  "query" : {
         <KairosDB query object>
  }
}
```

A successful request will return, for example,

```
{
  "status" : "ok",
  "queries" : [ {
       "sample_size" : 0,
       "results" : [ {
         "name" : "integration.tests",
         "tags" : {
           "test_type" : [ "integration" ]
         },
         "values" : [ ]
       } ]
  } ]
}
```

### *Query Metric Tags*
This operation will perform a query but only return the tag information. A JSON object parameter `query` is a KairosDB query used to
specify what points are to be returned.

```
{
  "action" : "query_metric_tags",
  "query" : {
         <KairosDB query object>
  }
}
```

A successful request will return

```
{
  "status" : "ok",
  "queries" : [ {
      "results" : [ {
        "name" : "integration.tests",
        "tags" : {
          "test_type" : [ "integration" ]
        },
        "values" : [ ]
      } ]
  } ]
}
```

### *Version*

To obtain a description of the KairosDB version send the following message to the module address

```
{
    "action": "version"
}
```

which will return a message describing the version of the database

```
{
  "version" : "KairosDB 0.9.2.20131022123502",
  "status" : "ok"
}
```

## Errors

For all operations if an error occurs the following response is returned

```
{
    "status": "error",
    "message": <message>
}
```
where `message` is the error message 
