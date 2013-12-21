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

package com.jonnywray.vertx;

import io.netty.handler.codec.http.HttpHeaders;
import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.json.JsonObject;

/**
 * Verticle implementing persistence service to the <a href="https://code.google.com/p/kairosdb/">KairosDB time series database</a>
 *
 * @author Jonny Wray
 */
public class KairosPersistor extends BusModBase implements Handler<Message<JsonObject>>{

    protected String address;
    protected String host;
    protected int port;

    protected HttpClient client;

    @Override
    public void start() {
        super.start();

        address = getOptionalStringConfig("address", "jonnywray.kairospersistor");
        host = getOptionalStringConfig("host", "localhost");
        port = getOptionalIntConfig("port", 8080);

        client = vertx.createHttpClient()
                .setPort(port)
                .setHost(host)
                .setKeepAlive(true)
                .setSSL(false);
        eb.registerHandler(address, this);
    }

    @Override
    public void stop(){
        if(client != null){
            client.close();
        }
    }

    public void handle(Message<JsonObject> message) {
        String action = message.body().getString("action");
        if (action == null) {
            sendError(message, "action must be specified");
            return;
        }
        switch (action){
            case "add_data_points" :
                addDataPoints(message);
                break;
            case "delete_metric":
                deleteMetric(message);
                break;
            case "list_metric_names":
                listMetricNames(message);
                break;
            case "list_tag_names":
                listTagNames(message);
                break;
            case "list_tag_values":
                listTagValues(message);
                break;
            case "query_metrics":
                queryMetrics(message);
                break;
            case "query_metric_tags":
                queryMetricTags(message);
                break;
            case "version" :
                version(message);
                break;
            default:
                sendError(message, "unsupported action specified: "+action);
        }
    }

    private void queryMetrics(final Message<JsonObject> message){
        JsonObject query = message.body().getObject("query");
        if (query == null) {
            sendError(message, "metric query must be specified");
            return;
        }
        HttpClientRequest request = client.post("/api/v1/datapoints/query", new Handler<HttpClientResponse>() {
            @Override
            public void handle(final HttpClientResponse response) {
                response.bodyHandler(new Handler<Buffer>() {
                    public void handle(Buffer body) {
                        int responseCode = response.statusCode();
                        if (responseCode == 200) {
                            JsonObject responseObject = new JsonObject(body.toString());
                            sendOK(message, responseObject);
                        } else {
                            String errorMessage = "error querying metrics from KairosDB: " + response.statusCode() + " " + response.statusMessage();
                            container.logger().error(errorMessage);
                            sendError(message, errorMessage);
                        }
                    }
                });
            }
        });
        String encodedObject = query.encode();
        request.putHeader(HttpHeaders.Names.CONTENT_TYPE, "application/json")
                .putHeader(HttpHeaders.Names.CONTENT_LENGTH, Integer.toString(encodedObject.getBytes().length))
                .write(encodedObject)
                .end();
    }

    private void queryMetricTags(final Message<JsonObject> message){

    }

    private void deleteMetric(final Message<JsonObject> message) {
        String metricName = message.body().getString("metric_name");
        if (metricName == null) {
            sendError(message, "metric name must be specified");
            return;
        }
        HttpClientRequest request = client.delete("/api/v1/metric/"+metricName, new Handler<HttpClientResponse>() {
            @Override
            public void handle(final HttpClientResponse response) {
            response.bodyHandler(new Handler<Buffer>() {
                public void handle(Buffer body) {
                int responseCode = response.statusCode();
                if (responseCode == 204) {
                    sendOK(message);
                }
                else{
                    String errorMessage =  "unexpected response deleting metric from KairosDB: " + response.statusCode() + " " + response.statusMessage();
                    container.logger().error(errorMessage);
                    sendError(message, errorMessage);
                }
                }
            });
            }
        });
        request.end();
    }

    private void version(final Message<JsonObject> message) {
        HttpClientRequest request = client.get("/api/v1/version", new Handler<HttpClientResponse>() {
            @Override
            public void handle(final HttpClientResponse response) {
                response.bodyHandler(new Handler<Buffer>() {
                    public void handle(Buffer body) {
                        int responseCode = response.statusCode();
                        if (responseCode == 200) {
                            JsonObject responseObject = new JsonObject(body.toString());
                            sendOK(message, responseObject);
                        }
                        else{
                            String errorMessage =  "unexpected response requesting version KairosDB: " + response.statusCode() + " " + response.statusMessage();
                            container.logger().error(errorMessage);
                            sendError(message, errorMessage);
                        }
                    }
                });
            }
        });
        request.end();
    }

    private void addDataPoints(final Message<JsonObject> message) {
        HttpClientRequest request = client.post("/api/v1/datapoints/", new Handler<HttpClientResponse>() {
            @Override
            public void handle(HttpClientResponse response) {
            int responseCode = response.statusCode();
            if (responseCode == 204) {
                sendOK(message);
            }
            else{
                String errorMessage =  "unexpected response sending data points to KairosDB: " + response.statusCode() + " " + response.statusMessage();
                container.logger().error(errorMessage);
                sendError(message, errorMessage);
            }
            }
        });

        JsonObject dataPoints = message.body().getObject("datapoints");
        if(dataPoints == null){
            sendError(message, "data points object is not specified");
            return;
        }
        JsonValidator validator = new JsonValidator();
        if(!validator.validateDataPoints(dataPoints)){
            sendError(message, "data points object was incorrectly formatted");
            return;
        }
        String encodedObject = dataPoints.encode();
        request.putHeader(HttpHeaders.Names.CONTENT_TYPE, "application/json")
                .putHeader(HttpHeaders.Names.CONTENT_LENGTH, Integer.toString(encodedObject.getBytes().length))
                .write(encodedObject)
                .end();
    }


    private void listMetricNames(final Message<JsonObject> message) {
        HttpClientRequest request = client.get("/api/v1/metricnames", new Handler<HttpClientResponse>() {
            @Override
            public void handle(final HttpClientResponse response) {
                response.bodyHandler(new Handler<Buffer>() {
                    public void handle(Buffer body) {
                        int responseCode = response.statusCode();
                        if (responseCode == 200) {
                            JsonObject responseObject = new JsonObject(body.toString());
                            sendOK(message, responseObject);
                        }
                        else{
                            String errorMessage =  "unexpected response requesting metric names KairosDB: " + response.statusCode() + " " + response.statusMessage();
                            container.logger().error(errorMessage);
                            sendError(message, errorMessage);
                        }
                    }
                });
            }
        });
        request.end();
    }

    private void listTagNames(final Message<JsonObject> message) {
        HttpClientRequest request = client.get("/api/v1/tagnames", new Handler<HttpClientResponse>() {
            @Override
            public void handle(final HttpClientResponse response) {
                response.bodyHandler(new Handler<Buffer>() {
                    public void handle(Buffer body) {
                        int responseCode = response.statusCode();
                        if (responseCode == 200) {
                            JsonObject responseObject = new JsonObject(body.toString());
                            sendOK(message, responseObject);
                        }
                        else{
                            String errorMessage =  "unexpected response requesting tag names KairosDB: " + response.statusCode() + " " + response.statusMessage();
                            container.logger().error(errorMessage);
                            sendError(message, errorMessage);
                        }
                    }
                });
            }
        });
        request.end();
    }

    private void listTagValues(final Message<JsonObject> message) {
        HttpClientRequest request = client.get("/api/v1/tagvalues", new Handler<HttpClientResponse>() {
            @Override
            public void handle(final HttpClientResponse response) {
                response.bodyHandler(new Handler<Buffer>() {
                    public void handle(Buffer body) {
                        int responseCode = response.statusCode();
                        if (responseCode == 200) {
                            JsonObject responseObject = new JsonObject(body.toString());
                            sendOK(message, responseObject);
                        }
                        else{
                            String errorMessage =  "unexpected response requesting tag values KairosDB: " + response.statusCode() + " " + response.statusMessage();
                            container.logger().error(errorMessage);
                            sendError(message, errorMessage);
                        }
                    }
                });
            }
        });
        request.end();
    }

}
