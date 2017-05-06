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
import java.util.HashSet;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//TODO: this should be an internal enum
import org.apache.storm.generated.Window;

public class Aggregation {
    private final static Logger LOG = LoggerFactory.getLogger(Aggregation.class);

    // Key components
    private HashMap<String, Object> settings;

    public Aggregation() {
        settings = new HashMap<String, Object>();
    }

    // Filter for specific fields
    // Todo: Filter for different instances of the same field, two hosts for example

    public void filterMetric(String metric) {
        HashSet<String> metricSet = (HashSet<String>)this.settings.get(StringKeywords.metricSet);
        if (metricSet == null){
            metricSet = new HashSet<String>();
            this.settings.put(StringKeywords.metricSet, metricSet);
        }
        metricSet.add(metric);
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

    public void filterAggLevel(Integer comp) {
        // TODO: ugly, make this an enum or something until it hits the store
        this.settings.put(StringKeywords.aggLevel, comp);
    }

    public HashMap<String, Object> getSettings(){
        return this.settings;
    }

    public void filterTime(Long timeStart, Long timeEnd, Window window) {
        HashSet<TimeRange> timeRangeSet = (HashSet<TimeRange>)this.settings.get(StringKeywords.timeRangeSet);
        if (timeRangeSet == null){
            timeRangeSet = new HashSet<TimeRange>();
            this.settings.put(StringKeywords.timeRangeSet, timeRangeSet);
        }
        TimeRange timeRange = new TimeRange(timeStart, timeEnd, window);
        timeRangeSet.add(timeRange);
    }

    public void raw(MetricStore store, IAggregator agg) throws MetricException {
        MetricResult result = new MetricResult();
        store.scan (settings, agg);
    }
    // Aggregations

    public MetricResult sum(MetricStore store) throws MetricException {
        MetricResult result = new MetricResult();
        store.scan (settings, (metric, timeRanges) -> {
            String metricName = metric.getMetricName();
            for (TimeRange tr : timeRanges) {
                Double value = result.getValueFor(metricName, tr);
                value = value == null ? 0.0 : value;
                Double newValue = value + metric.getValue();
                result.setValueFor(metricName, tr, newValue);
                result.incCountFor(metricName, tr);
            }
        });
        return result;
    }

    public MetricResult min(MetricStore store) throws MetricException {
        MetricResult result = new MetricResult();
        store.scan(settings, (metric, timeRanges) -> {
            String metricName = metric.getMetricName();
            for (TimeRange tr : timeRanges) {
                Double value = metric.getValue();
                value = value == null ? 0.0 : value;
                result.setValueFor(metricName, tr, Math.min(value, result.getValueFor(metricName, tr)));
                result.incCountFor(metricName, tr);
            }
        });
        return result;
    }

    public MetricResult max(MetricStore store) throws MetricException {
        MetricResult result = new MetricResult();
        store.scan(settings, (metric, timeRanges) -> {
            String metricName = metric.getMetricName();
            for (TimeRange tr : timeRanges) {
                Double value = metric.getValue();
                result.setValueFor(metricName, tr, Math.max(value, result.getValueFor(metricName, tr)));
                result.incCountFor(metricName, tr);
            }
        });
        return result;
    }

    public MetricResult mean(MetricStore store) throws MetricException {
        MetricResult result = new MetricResult();
        store.scan(settings, (metric, timeRanges) -> {
            String metricName = metric.getMetricName();
            for (TimeRange tr : timeRanges) {
                // for an hourly bucket, this is a sum
                Double value = metric.getValue();
                //TODO: if (the metric isn't in the settings, don't include it in the result) {
                //TODO:     // OR split the result into sums, avgs, maxes, etc. ==> easier
                //TODO: }
                value = value == null ? 0.0 : value;
                Double prev = result.getValueFor(metricName, tr);
                prev = prev == null ? 0.0 : prev;
                result.setValueFor(metricName, tr, value + prev);
                result.incCountFor(metricName, tr, metric.getCount());
            }
        });
        LOG.info("try to perform avg for {}", result.getMetricNames());
        for (String metricName : result.getMetricNames()) {
            LOG.info("try to perform avg for {}", metricName);
            for (TimeRange tr : result.getTimeRanges(metricName)){
                LOG.info("try to perform avg for {} at {}", metricName, tr);
                Long count = result.getCountFor(metricName, tr);
                LOG.info("count for avg for {} at {} = {}", metricName, tr, count);
                if (count != null && count > 0) {
                    LOG.info("perform avg for {} {} {} / {} = {}", metricName, tr, result.getValueFor(metricName, tr), count);
                    result.setValueFor(metricName, tr, 
                            result.getValueFor(metricName, tr) / 
                            count);
                }
            }
        }
        return result;
    }
}
