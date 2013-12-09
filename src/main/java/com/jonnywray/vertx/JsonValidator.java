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

import org.vertx.java.core.json.JsonObject;

/**
 * Helper class responsible for validating JSON objects that are to be passed as commands to the KairosDB REST interface
 *
 * @author Jonny Wray
 */
public class JsonValidator {


    public boolean validateDataPoints(JsonObject dataPoints){
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
