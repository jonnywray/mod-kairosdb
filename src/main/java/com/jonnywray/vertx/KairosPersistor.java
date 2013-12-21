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

    private static final String JSON_CONTENT_TYPE = "application/json";


    private static final String BASE_URI = "/api/v1/";
    private static final String ADD_DATAPOINTS_URI = BASE_URI + "datapoints";
    private static final String DELETE_DATAPOINTS_URI = BASE_URI + "datapoints/delete";
    private static final String QUERY_DATAPOINTS_URI = BASE_URI + "datapoints/query";
    private static final String QUERY_DATAPOINTS_TAGS_URI = BASE_URI + "datapoints/query/tags";

    private static final String DELETE_METRIC_URI = BASE_URI + "metric/%s";
    private static final String VERSION_URI = BASE_URI + "version";

    private static final String METRIC_NAMES_URI = BASE_URI + "metricnames";
    private static final String TAG_NAMES_URI = BASE_URI + "tagnames";
    private static final String TAG_VALUES_URI = BASE_URI + "tagvalues";

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
            case "delete_data_points":
                deleteDataPoints(message);
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

    private void deleteDataPoints(final Message<JsonObject> message){
        JsonObject query = message.body().getObject("query");
        if (query == null) {
            sendError(message, "metric query must be specified");
            return;
        }
        HttpClientRequest request = client.post(DELETE_DATAPOINTS_URI, new Handler<HttpClientResponse>() {
            @Override
            public void handle(final HttpClientResponse response) {
                response.bodyHandler(new Handler<Buffer>() {
                    public void handle(Buffer body) {
                        int responseCode = response.statusCode();
                        if (responseCode == 204) {
                            sendOK(message);
                        }
                        else{
                            String errorMessage =  "error deleting data points: " + response.statusCode() + " " + response.statusMessage();
                            container.logger().error(errorMessage);
                            sendError(message, errorMessage);
                        }
                    }
                });
            }
        });
        String encodedObject = query.encode();
        request.putHeader(HttpHeaders.Names.CONTENT_TYPE, JSON_CONTENT_TYPE)
                .putHeader(HttpHeaders.Names.CONTENT_LENGTH, Integer.toString(encodedObject.getBytes().length))
                .write(encodedObject)
                .end();
    }

    private void queryMetrics(final Message<JsonObject> message){
        JsonObject query = message.body().getObject("query");
        if (query == null) {
            sendError(message, "metric query must be specified");
            return;
        }
        HttpClientRequest request = client.post(QUERY_DATAPOINTS_URI, new Handler<HttpClientResponse>() {
            @Override
            public void handle(final HttpClientResponse response) {
                response.bodyHandler(new Handler<Buffer>() {
                    public void handle(Buffer body) {
                        int responseCode = response.statusCode();
                        if (responseCode == 200) {
                            JsonObject responseObject = new JsonObject(body.toString());
                            sendOK(message, responseObject);
                        } else {
                            String errorMessage = "error querying metrics: " + response.statusCode() + " " + response.statusMessage();
                            container.logger().error(errorMessage);
                            sendError(message, errorMessage);
                        }
                    }
                });
            }
        });
        String encodedObject = query.encode();
        request.putHeader(HttpHeaders.Names.CONTENT_TYPE, JSON_CONTENT_TYPE)
                .putHeader(HttpHeaders.Names.CONTENT_LENGTH, Integer.toString(encodedObject.getBytes().length))
                .write(encodedObject)
                .end();
    }

    private void queryMetricTags(final Message<JsonObject> message){
        JsonObject query = message.body().getObject("query");
        if (query == null) {
            sendError(message, "metric query must be specified");
            return;
        }
        HttpClientRequest request = client.post(QUERY_DATAPOINTS_TAGS_URI, new Handler<HttpClientResponse>() {
            @Override
            public void handle(final HttpClientResponse response) {
                response.bodyHandler(new Handler<Buffer>() {
                    public void handle(Buffer body) {
                        int responseCode = response.statusCode();
                        if (responseCode == 200) {
                            JsonObject responseObject = new JsonObject(body.toString());
                            sendOK(message, responseObject);
                        } else {
                            String errorMessage = "error querying metric tags: " + response.statusCode() + " " + response.statusMessage();
                            container.logger().error(errorMessage);
                            sendError(message, errorMessage);
                        }
                    }
                });
            }
        });
        String encodedObject = query.encode();
        request.putHeader(HttpHeaders.Names.CONTENT_TYPE, JSON_CONTENT_TYPE)
                .putHeader(HttpHeaders.Names.CONTENT_LENGTH, Integer.toString(encodedObject.getBytes().length))
                .write(encodedObject)
                .end();
    }

    private void deleteMetric(final Message<JsonObject> message) {
        String metricName = message.body().getString("metric_name");
        if (metricName == null) {
            sendError(message, "metric name must be specified");
            return;
        }
        HttpClientRequest request = client.delete(String.format(DELETE_METRIC_URI, metricName), new Handler<HttpClientResponse>() {
            @Override
            public void handle(final HttpClientResponse response) {
            response.bodyHandler(new Handler<Buffer>() {
                public void handle(Buffer body) {
                int responseCode = response.statusCode();
                if (responseCode == 204) {
                    sendOK(message);
                }
                else{
                    String errorMessage =  "error deleting metric: " + response.statusCode() + " " + response.statusMessage();
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
        HttpClientRequest request = client.get(VERSION_URI, new Handler<HttpClientResponse>() {
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
                            String errorMessage =  "error requesting version: " + response.statusCode() + " " + response.statusMessage();
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
        HttpClientRequest request = client.post(ADD_DATAPOINTS_URI, new Handler<HttpClientResponse>() {
            @Override
            public void handle(HttpClientResponse response) {
            int responseCode = response.statusCode();
            if (responseCode == 204) {
                sendOK(message);
            }
            else{
                String errorMessage =  "error adding data points: " + response.statusCode() + " " + response.statusMessage();
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
        request.putHeader(HttpHeaders.Names.CONTENT_TYPE, JSON_CONTENT_TYPE)
                .putHeader(HttpHeaders.Names.CONTENT_LENGTH, Integer.toString(encodedObject.getBytes().length))
                .write(encodedObject)
                .end();
    }


    private void listMetricNames(final Message<JsonObject> message) {
        HttpClientRequest request = client.get(METRIC_NAMES_URI, new Handler<HttpClientResponse>() {
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
                            String errorMessage =  "error listing metric names: " + response.statusCode() + " " + response.statusMessage();
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
        HttpClientRequest request = client.get(TAG_NAMES_URI, new Handler<HttpClientResponse>() {
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
                            String errorMessage =  "error listing tag names: " + response.statusCode() + " " + response.statusMessage();
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
        HttpClientRequest request = client.get(TAG_VALUES_URI, new Handler<HttpClientResponse>() {
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
                            String errorMessage =  "error listing tag values: " + response.statusCode() + " " + response.statusMessage();
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
