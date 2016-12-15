/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.metrics2.store;

import java.util.List;
import java.util.HashMap;

public class Aggregation {

    // Store component
    private MetricStore store;

    // Key components
    private HashMap<String, Object> settings;

    public Aggregation(MetricStore store) {
        this.store = store;
        settings = new HashMap<String, Object>();
    }

    // Filter for specific fields
    // Todo: Filter for different instances of the same field, two hosts for example

    public void filterMetric(String metric) {
        this.settings.put(StringKeywords.metricName, metric);
    }

    public void filterTopo(String topoId) {
        this.settings.put(StringKeywords.topoId, topoId);
    }

    public void filterHost(String host) {
        this.settings.put(StringKeywords.host, host);
    }

    public void filterPort(String port) {
        this.settings.put(StringKeywords.port, port);
    }

    public void filterComp(String comp) {
        this.settings.put(StringKeywords.component, comp);
    }

    public void filterTimeStart(Long time) { 
        this.settings.put(StringKeywords.timeStart, time);
    }

    public void filterTimeEnd(Long time) { 
        this.settings.put(StringKeywords.timeEnd, time);
    }

    public void filterTime(Long timeStart, Long timeEnd) {
        this.settings.put(StringKeywords.timeStart, timeStart);
        this.settings.put(StringKeywords.timeEnd, timeEnd);
    }

    // Aggregations

    public Double sum() throws MetricException {
        Double sum = 0.0;
        List<Double> x = this.store.scan(settings);
        for(Double each : x) {
            sum += each;
        }
        return sum;
    }

    public Double min() throws MetricException {
        Double min = Double.MAX_VALUE;
        List<Double> x = this.store.scan(settings);
        for(Double curr : x) {
            if(curr < min) {
                min = curr;
            }
        }
        return min;
    }

    public Double max() throws MetricException {
        Double max = Double.MIN_VALUE;
        List<Double> x = this.store.scan(settings);
        for(Double curr : x) {
            if(curr > max) {
                max = curr;
            }
        }
        return max;
    }

    public Double mean() throws MetricException {
        Double sum = 0.0;
        Integer count = 0;
        List<Double> x = this.store.scan(settings);
        for(Double each : x) {
            sum += each;
            count++;
        }
        return sum / count;
    }

}
