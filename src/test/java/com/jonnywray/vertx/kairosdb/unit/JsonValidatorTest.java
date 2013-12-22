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

package com.jonnywray.vertx.kairosdb.unit;

import com.jonnywray.vertx.kairosdb.JsonValidator;
import org.junit.Test;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import static org.junit.Assert.*;

/**
 * Unit tests of the validator
 *
 * @author Jonny Wray
 */
public class JsonValidatorTest {

    @Test
    public void testSingleDataPoint(){
        JsonValidator validator = new JsonValidator();
        boolean valid = validator.validateDataPoints(exampleSingleDataPoint());
        assertTrue("Single data point is not valid when it should be", valid);
    }

    @Test
    public void testInvalidSingleDataPointNoName(){
        JsonValidator validator = new JsonValidator();
        JsonObject data = exampleSingleDataPoint();
        data.removeField("name");
        boolean valid = validator.validateDataPoints(data);
        assertFalse("Single data point is valid when it should not be", valid);
    }

    @Test
    public void testInvalidSingleDataPointNoTimestamp(){
        JsonValidator validator = new JsonValidator();
        JsonObject data = exampleSingleDataPoint();
        data.removeField("timestamp");
        boolean valid = validator.validateDataPoints(data);
        assertFalse("Single data point is valid when it should not be", valid);
    }

    @Test
    public void testInvalidSingleDataPointNoValue(){
        JsonValidator validator = new JsonValidator();
        JsonObject data = exampleSingleDataPoint();
        data.removeField("value");
        boolean valid = validator.validateDataPoints(data);
        assertFalse("Single data point is valid when it should not be", valid);
    }

    @Test
    public void testInvalidSingleDataPointNoTags(){
        JsonValidator validator = new JsonValidator();
        JsonObject data = exampleSingleDataPoint();
        data.removeField("tags");
        boolean valid = validator.validateDataPoints(data);
        assertFalse("Single data point is valid when it should not be", valid);
    }

    @Test
    public void testMultipleDataPoint(){
        JsonValidator validator = new JsonValidator();
        boolean valid = validator.validateDataPoints(exampleMultipleDataPoint());
        assertTrue("Multiple data point is not valid when it should be", valid);
    }

    @Test
    public void testInvalidMultipleDataPointNoName(){
        JsonValidator validator = new JsonValidator();
        JsonObject data = exampleMultipleDataPoint();
        data.removeField("name");
        boolean valid = validator.validateDataPoints(data);
        assertFalse("Multiple data point is valid when it should not be", valid);
    }

    @Test
    public void testInvalidMultipleDataPointNoTags(){
        JsonValidator validator = new JsonValidator();
        JsonObject data = exampleMultipleDataPoint();
        data.removeField("tags");
        boolean valid = validator.validateDataPoints(data);
        assertFalse("Multiple data point is valid when it should not be", valid);
    }

    @Test
    public void testInvalidMultipleDataPointNoDatapoints(){
        JsonValidator validator = new JsonValidator();
        JsonObject data = exampleMultipleDataPoint();
        data.removeField("datapoints");
        boolean valid = validator.validateDataPoints(data);
        assertFalse("Multiple data point is valid when it should not be", valid);
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
        dataPoints.putObject("tags", exampleTags());
        return dataPoints;
    }

    private JsonObject exampleSingleDataPoint(){

        JsonObject dataPoints = new JsonObject();
        dataPoints.putString("name", "integration.tests");
        dataPoints.putNumber("timestamp", System.currentTimeMillis());
        dataPoints.putNumber("value", 42);
        dataPoints.putObject("tags", exampleTags());
        return dataPoints;
    }

    private JsonObject exampleTags(){

        JsonObject tags = new JsonObject();
        tags.putString("test_type", "integration");
        return tags;
    }
}

