/*
 * Copyright 2013 Jonny Wray
 *
 *  Jonny Wray licenses this file to you under the Apache License, version 2.0
 *  (the "License"); you may not use this file except in compliance with the
 *  License.  You may obtain a copy of the License at:
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 *  License for the specific language governing permissions and limitations
 *  under the License.
 *
 *  @author <a href="http://www.jonnywray.com">Jonny Wray</a>
 */

package com.jonnywray.vertx.integration.java;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import static org.vertx.testtools.VertxAssert.*;

/**
 * Integration tests for the {@link com.jonnywray.vertx.KairosPersistor} against an externally running KairosDB with the
 * default configuration
 *
 * TODO: once various pieces are written write test sequences: insert query delete query
 *
 * @author Jonny Wray
 */
public class KairosPersistorIT extends TestVerticle {

    private static final long ONE_DAY = 1000 * 60 * 60 * 24;

    /**
     * Test using an empty query
     */
    @Test
    public void testInvalidEmptyQueryMetricTags() {
        JsonObject validQuery = new JsonObject();
        JsonObject commandObject = new JsonObject();
        commandObject.putString("action", "query_metric_tags");
        commandObject.putObject("query", validQuery);
        vertx.eventBus().send("jonnywray.kairospersistor", commandObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                JsonObject response = reply.body();
                assertTrue("Response status is null", response.getString("status") != null);
                assertEquals("Response status is not error", "error", response.getString("status"));
                assertEquals("Response message is not correct", "error querying metrics from KairosDB: 400 Bad Request", response.getString("message"));
                testComplete();
            }
        });
    }

    /**
     * Test using a query missing the tag definitions
     */
    @Test
    public void testInvalidPartialQueryMetricTags() {
        JsonObject commandObject = new JsonObject();
        commandObject.putString("action", "query_metric_tags");
        commandObject.putObject("query", invalidMetricQuery());
        vertx.eventBus().send("jonnywray.kairospersistor", commandObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                JsonObject response = reply.body();
                assertTrue("Response status is null", response.getString("status") != null);
                assertEquals("Response status is not error", "error", response.getString("status"));
                assertEquals("Response message is not correct", "error querying metrics from KairosDB: 400 Bad Request", response.getString("message"));
                testComplete();
            }
        });
    }

    /**
     * Test using a valid query. Note that the response array name here is based on tests and disagrees with the documentation.
     */
    @Test
    public void testValidQueryMetricTags() {

        JsonObject commandObject = new JsonObject();
        commandObject.putString("action", "query_metric_tags");
        commandObject.putObject("query", validMetricQuery());
        vertx.eventBus().send("jonnywray.kairospersistor", commandObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                JsonObject response = reply.body();
                System.out.println(response.encodePrettily());
                assertTrue("Response status is null", response.getString("status") != null);
                assertEquals("Response status is not ok", "ok", response.getString("status"));
                assertTrue("Response queries array is null", response.getArray("queries") != null);
                testComplete();
            }
        });
    }


    /**
     * Test using an empty query
     */
    @Test
    public void testInvalidEmptyQueryMetrics() {
        JsonObject validQuery = new JsonObject();
        JsonObject commandObject = new JsonObject();
        commandObject.putString("action", "query_metrics");
        commandObject.putObject("query", validQuery);
        vertx.eventBus().send("jonnywray.kairospersistor", commandObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                JsonObject response = reply.body();
                assertTrue("Response status is null", response.getString("status") != null);
                assertEquals("Response status is not error", "error", response.getString("status"));
                assertEquals("Response message is not correct", "error querying metrics from KairosDB: 400 Bad Request", response.getString("message"));
                testComplete();
            }
        });
    }

    /**
     * Test using a query missing the tag definitions
     */
    @Test
    public void testInvalidPartialQueryMetrics() {
        JsonObject commandObject = new JsonObject();
        commandObject.putString("action", "query_metrics");
        commandObject.putObject("query", invalidMetricQuery());
        vertx.eventBus().send("jonnywray.kairospersistor", commandObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                JsonObject response = reply.body();
                assertTrue("Response status is null", response.getString("status") != null);
                assertEquals("Response status is not error", "error", response.getString("status"));
                assertEquals("Response message is not correct", "error querying metrics from KairosDB: 400 Bad Request", response.getString("message"));
                testComplete();
            }
        });
    }

    /**
     * Test using a valid query
     */
    @Test
    public void testValidQueryMetrics() {

        JsonObject commandObject = new JsonObject();
        commandObject.putString("action", "query_metrics");
        commandObject.putObject("query", validMetricQuery());
        vertx.eventBus().send("jonnywray.kairospersistor", commandObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                JsonObject response = reply.body();
                assertTrue("Response status is null", response.getString("status") != null);
                assertEquals("Response status is not ok", "ok", response.getString("status"));
                assertTrue("Response metrics array is null", response.getArray("queries") != null);
                testComplete();
            }
        });
    }


    @Test
    public void testDeleteMetricFakeMetric() {
        JsonObject commandObject = new JsonObject();
        commandObject.putString("action", "delete_metric");
        commandObject.putString("metric_name", "fake.metric");
        vertx.eventBus().send("jonnywray.kairospersistor", commandObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                JsonObject response = reply.body();
                assertTrue("Response status is null", response.getString("status") != null);
                assertEquals("Response status is not ok", "ok", response.getString("status"));
                testComplete();
            }
        });
    }

    @Test
    public void testDeleteMetricNoName() {
        JsonObject commandObject = new JsonObject();
        commandObject.putString("action", "delete_metric");
        vertx.eventBus().send("jonnywray.kairospersistor", commandObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                JsonObject response = reply.body();
                assertTrue("Response status is null", response.getString("status") != null);
                assertEquals("Response status is not error", "error", response.getString("status"));
                assertEquals("Response message is not correct", "metric name must be specified", response.getString("message"));
                testComplete();
            }
        });
    }


    @Test
    public void testListTagNames() {
        JsonObject commandObject = new JsonObject();
        commandObject.putString("action", "list_tag_names");
        vertx.eventBus().send("jonnywray.kairospersistor", commandObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                JsonObject response = reply.body();
                assertTrue("Response status is null", response.getString("status") != null);
                assertEquals("Response status is not ok", "ok", response.getString("status"));
                assertTrue("Response has no results", response.getArray("results") != null);
                testComplete();
            }
        });
    }

    @Test
    public void testListTagValues() {
        JsonObject commandObject = new JsonObject();
        commandObject.putString("action", "list_tag_values");
        vertx.eventBus().send("jonnywray.kairospersistor", commandObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                JsonObject response = reply.body();
                assertTrue("Response status is null", response.getString("status") != null);
                assertEquals("Response status is not ok", "ok", response.getString("status"));
                assertTrue("Response has no results", response.getArray("results") != null);
                testComplete();
            }
        });
    }

    @Test
    public void testListMetricNames() {
        JsonObject commandObject = new JsonObject();
        commandObject.putString("action", "list_metric_names");
        vertx.eventBus().send("jonnywray.kairospersistor", commandObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                JsonObject response = reply.body();
                assertTrue("Response status is null", response.getString("status") != null);
                assertEquals("Response status is not ok", "ok", response.getString("status"));
                assertTrue("Response has no results", response.getArray("results") != null);
                testComplete();
            }
        });
    }

    @Test
    public void testVersion() {
        JsonObject commandObject = new JsonObject();
        commandObject.putString("action", "version");
        vertx.eventBus().send("jonnywray.kairospersistor", commandObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                JsonObject response = reply.body();
                assertTrue("Response status is null", response.getString("status") != null);
                assertEquals("Response status is not ok", "ok", response.getString("status"));
                assertTrue("Response version is null", response.getString("version") != null);
                assertEquals("Response version is not correct", "KairosDB 0.9.2.20131022123502", response.getString("version"));
                testComplete();
            }
        });
    }

    @Test
    public void testAddDataPointsSingle() {


        JsonObject commandObject = new JsonObject();
        commandObject.putString("action", "add_data_points");

        JsonObject dataPoints = exampleSingleDataPoint();
        dataPoints.putObject("tags", exampleTags());
        commandObject.putObject("datapoints", dataPoints);

        vertx.eventBus().send("jonnywray.kairospersistor", commandObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                JsonObject response = reply.body();
                assertTrue("Response status is null", response.getString("status") != null);
                assertEquals("Response status is not ok", "ok", response.getString("status"));
                testComplete();
            }
        });
    }

    @Test
    public void testAddDataPointsMultiple() {
        JsonObject commandObject = new JsonObject();
        commandObject.putString("action", "add_data_points");

        JsonObject dataPoints = exampleMultipleDataPoint();
        dataPoints.putObject("tags", exampleTags());
        commandObject.putObject("datapoints", dataPoints);

        vertx.eventBus().send("jonnywray.kairospersistor", commandObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                JsonObject response = reply.body();
                assertTrue("Response status is null", response.getString("status") != null);
                assertEquals("Response status is not ok", "ok", response.getString("status"));
                testComplete();
            }
        });
    }

    @Test
    public void testAddDataPointsWithEmptyCommand() {
        JsonObject commandObject = new JsonObject();
        commandObject.putString("action", "add_data_points");
        vertx.eventBus().send("jonnywray.kairospersistor", commandObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                JsonObject response = reply.body();
                assertTrue("Response status is null", response.getString("status") != null);
                assertEquals("Response status is not error", "error", response.getString("status"));
                assertEquals("Response message is not correct", "data points object is not specified", response.getString("message"));
                testComplete();
            }
        });
    }

    @Test
    public void testAddDataPointsSingleNoTags() {
        JsonObject commandObject = new JsonObject();
        commandObject.putString("action", "add_data_points");
        commandObject.putObject("datapoints", exampleSingleDataPoint());

        vertx.eventBus().send("jonnywray.kairospersistor", commandObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                JsonObject response = reply.body();
                assertTrue("Response status is null", response.getString("status") != null);
                assertEquals("Response status is not error", "error", response.getString("status"));
                testComplete();
            }
        });
    }


    @Test
    public void testInvalidAction() {
        JsonObject commandObject = new JsonObject();
        commandObject.putString("action", "invalid");
        vertx.eventBus().send("jonnywray.kairospersistor", commandObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                JsonObject response = reply.body();
                assertTrue("Response status is null", response.getString("status") != null);
                assertEquals("Response status is not error", "error", response.getString("status"));
                assertEquals("Response message is not correct", "unsupported action specified: invalid", response.getString("message"));
                testComplete();
            }
        });
    }

    /*
     * Valid query with the minimal number of fields
     */
    private JsonObject validMetricQuery(){

        long now = System.currentTimeMillis();
        long oneDayPrevious = now - ONE_DAY;
        JsonObject validQuery = new JsonObject();
        validQuery.putNumber("start_absolute", oneDayPrevious);
        JsonObject end = new JsonObject();
        end.putString("value", "2");
        end.putString("unit", "days");
        validQuery.putObject("end_relative", end);
        JsonArray metrics = new JsonArray();
        JsonObject metric = new JsonObject();
        metric.putString("name", "integration.tests");

        validQuery.putArray("metrics", metrics);
        return validQuery;
    }

    private JsonObject invalidMetricQuery(){

        long now = System.currentTimeMillis();
        long oneDayPrevious = now - ONE_DAY;

        JsonObject query = new JsonObject();
        query.putNumber("start_absolute", oneDayPrevious);
        JsonObject end = new JsonObject();
        end.putString("value", "2");
        end.putString("unit", "days");
        query.putObject("end_relative", end);
        return query;
    }

    private JsonObject exampleMultipleDataPoint(){
        JsonArray points = new JsonArray();
        JsonArray first = new JsonArray();
        first.addNumber(System.currentTimeMillis()).addNumber(42);
        points.addArray(first);
        JsonArray second = new JsonArray();
        second.addNumber(System.currentTimeMillis() + 100).addNumber(52);
        points.addArray(second);

        JsonObject dataPoints = new JsonObject();
        dataPoints.putString("name", "integration.tests");
        dataPoints.putArray("datapoints", points);
        return dataPoints;
    }

    private JsonObject exampleSingleDataPoint(){

        JsonObject dataPoints = new JsonObject();
        dataPoints.putString("name", "integration.tests");
        dataPoints.putNumber("timestamp", System.currentTimeMillis());
        dataPoints.putNumber("value", 42);
        return dataPoints;
    }

    private JsonObject exampleTags(){

        JsonObject tags = new JsonObject();
        tags.putString("test_type", "integration");
        return tags;
    }


    @Override
    public void start() {
        initialize();
        container.deployModule(System.getProperty("vertx.modulename"), new AsyncResultHandler<String>() {
            @Override
            public void handle(AsyncResult<String> asyncResult) {
                assertTrue(asyncResult.succeeded());
                assertNotNull("deploymentID should not be null", asyncResult.result());
                startTests();
            }
        });
    }
}
