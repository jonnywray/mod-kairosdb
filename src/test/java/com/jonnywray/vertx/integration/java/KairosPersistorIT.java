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
 * @author Jonny Wray
 */
public class KairosPersistorIT extends TestVerticle {


    @Test
    public void testVersion() {
        JsonObject commandObject = new JsonObject();
        commandObject.putString("action", "version");
        vertx.eventBus().send("vertx.kairospersistor", commandObject, new Handler<Message<JsonObject>>() {
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

        vertx.eventBus().send("vertx.kairospersistor", commandObject, new Handler<Message<JsonObject>>() {
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

        vertx.eventBus().send("vertx.kairospersistor", commandObject, new Handler<Message<JsonObject>>() {
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
        vertx.eventBus().send("vertx.kairospersistor", commandObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                JsonObject response = reply.body();
                assertTrue("Response status is null", response.getString("status") != null);
                assertEquals("Response status is not error", "error", response.getString("status"));
                assertEquals("Response message is not correct", "Data points object is not specified", response.getString("message"));
                testComplete();
            }
        });
    }

    @Test
    public void testAddDataPointsSingleNoTags() {
        JsonObject commandObject = new JsonObject();
        commandObject.putString("action", "add_data_points");
        commandObject.putObject("datapoints", exampleSingleDataPoint());

        vertx.eventBus().send("vertx.kairospersistor", commandObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                JsonObject response = reply.body();
                assertTrue("Response status is null", response.getString("status") != null);
                assertEquals("Response status is not error", "error", response.getString("status"));
                testComplete();
            }
        });
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
