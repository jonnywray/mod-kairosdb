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

import java.util.HashMap;
import java.util.Map;

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
    private Map<String, Handler<Message<JsonObject>>> commandDelegates = new HashMap<>();

    @Override
    public void start() {
        super.start();

        address = getOptionalStringConfig("address", "vertx.kairospersistor");
        host = getOptionalStringConfig("host", "localhost");
        port = getOptionalIntConfig("port", 8080);

        client = vertx.createHttpClient()
                .setPort(port)
                .setHost(host)
                .setKeepAlive(true)
                .setSSL(false);
        initializeDelegates();
        eb.registerHandler(address, this);
    }

    @Override
    public void handle(Message<JsonObject> message) {
        String action = message.body().getString("action");
        if (action == null) {
            sendError(message, "action must be specified");
            return;
        }
        if(!commandDelegates.containsKey(action)){
            sendError(message, "action "+action+" is not supported");
            return;
        }
        Handler<Message<JsonObject>> delegate = commandDelegates.get(action);
        delegate.handle(message);
    }

    private void initializeDelegates(){
        commandDelegates.put("version", new VersionHandler());
        commandDelegates.put("add_data_points", new AddDataPointsHandler());
    }

    private class VersionHandler implements Handler<Message<JsonObject>>{

        @Override
        public void handle(final Message<JsonObject> message) {
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
                                String errorMessage =  "Unexpected response sending data points to KairosDB: " + response.statusCode() + " " + response.statusMessage();
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

    private class AddDataPointsHandler implements Handler<Message<JsonObject>>{

        @Override
        public void handle(final Message<JsonObject> message) {
            HttpClientRequest request = client.post("/api/v1/datapoints/", new Handler<HttpClientResponse>() {
                @Override
                public void handle(HttpClientResponse response) {
                    int responseCode = response.statusCode();
                    if (responseCode == 204) {
                        sendOK(message);
                    }
                    else{
                        String errorMessage =  "Unexpected response sending data points to KairosDB: " + response.statusCode() + " " + response.statusMessage();
                        container.logger().error(errorMessage);
                        sendError(message, errorMessage);
                    }
                }
            });

            JsonObject dataPoints = message.body().getObject("datapoints");
            if(dataPoints == null){
                sendError(message, "Data points object is not specified");
                return;
            }
            if(!validateDataPoints(dataPoints)){
                sendError(message, "Data points object was incorrectly formatted");
                return;
            }
            String encodedObject = dataPoints.encode();
            request.putHeader(HttpHeaders.Names.CONTENT_TYPE, "application/json")
                    .putHeader(HttpHeaders.Names.CONTENT_LENGTH, Integer.toString(encodedObject.getBytes().length))
                    .write(encodedObject)
                    .end();
        }

        private boolean validateDataPoints(JsonObject dataPoints){
            if(dataPoints == null){
                return false;
            }
            if(dataPoints.getString("name") == null){
                return false;
            }
            if(dataPoints.getObject("tags") == null || dataPoints.getObject("tags").size() == 0){
                return false;
            }
            boolean validSingle = dataPoints.getNumber("timestamp") != null && dataPoints.getNumber("value") != null;
            if(dataPoints.getArray("datapoints") == null && !validSingle){
                return false;
            }
            return true;
        }
    }

}
