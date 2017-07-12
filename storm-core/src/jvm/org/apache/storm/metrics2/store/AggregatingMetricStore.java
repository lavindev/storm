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

import org.apache.storm.generated.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class AggregatingMetricStore implements MetricStore {
    private final static Logger LOG = LoggerFactory.getLogger(AggregatingMetricStore.class);
    private MetricStore store;

    private List<Long> _buckets;

    private class BucketInfo {
        // NOTE: roundedEndTime <= endTime && roundedStartTime <= startTime
        // for non-negative start and end times
        long startTime;
        long endTime;
        long roundedStartTime;
        long roundedEndTime;

        BucketInfo(TimeRange t, long resolution) {
            startTime = (t.startTime != null) ? t.startTime : 0L;
            endTime = (t.endTime != null) ? t.endTime : System.currentTimeMillis();
            roundedEndTime = resolution * ((endTime - (resolution - 1L)) / resolution);
            roundedStartTime = resolution * ((startTime + (resolution - 1L)) / resolution);
        }

        public String toString() {
            return "(" + startTime + ", " + roundedStartTime + ", " + roundedEndTime + ", " + endTime + ")" + ((roundedEndTime <= roundedStartTime) ? " BAD" : " OK");
        }

    }

    // testing
    public List<Long> getBuckets() {
        return _buckets;
    }

    public MetricStore getUnderlyingStore() {
        return store;
    }
    // end testing

    public AggregatingMetricStore(MetricStore store) {
        this.store = store;
        _buckets = new ArrayList<>();
        _buckets.add(60L); // 60 minutes
        _buckets.add(10L); // 10 minutes
        _buckets.add(1L);  // 1 minutes
    }

    @Override
    public void prepare(Map config) {
        // For now, nothing to configure
    }

    @Override
    public void insert(Metric metric) {

        LOG.debug("Inserting {}", metric);
        store.insert(metric);

        // update aggregates for each bucket
        for (Long bucket : _buckets) {
            updateAggregate(metric, bucket);
        }
    }

    private void updateAggregate(Metric m, Long bucket) {

        Metric aggMetric       = new Metric(m);
        Long   metricTimestamp = aggMetric.getTimeStamp();
        Double metricValue     = aggMetric.getValue();

        long msToBucket      = 1000 * 60 * bucket;
        Long roundedToBucket = msToBucket * (metricTimestamp / msToBucket);

        // set new key
        aggMetric.setAggLevel(bucket.byteValue());
        aggMetric.setTimeStamp(roundedToBucket);

        // retrieve existing aggregation
        if (store.populateValue(aggMetric))
            aggMetric.updateAverage(metricValue);
        else
            aggMetric.setValue(metricValue);

        // insert updated metric
        LOG.debug("inserting {} min bucket {}", aggMetric, bucket);
        store.insert(aggMetric);
    }

    @Override
    public void scan(IAggregator agg) {
        store.scan(agg);
    }

    private Long getBucket(int i, TimeRange t) {

        Long res;
        long startTime = (t.startTime != null) ? t.startTime : 0L;
        long endTime   = (t.endTime != null) ? t.endTime : System.currentTimeMillis();
        long timeDelta = endTime - startTime;

        do {
            res = (i < _buckets.size()) ? _buckets.get(i) : 0L;
            ++i;
        } while (res * 60L * 1000L > timeDelta && timeDelta > 0);

        return res;
    }

    private void _scan(HashMap<String, Object> settings, IAggregator agg, TimeRange t, int bucketsIdx) {

        Long               res     = getBucket(bucketsIdx, t);
        HashSet<TimeRange> timeSet = new HashSet<TimeRange>();

        LOG.info("At _scan buckets with {} {} {} {}", settings, agg, t, res);

        if (res == 0) {
            timeSet.add(t);
            settings.put(StringKeywords.timeRangeSet, timeSet);
            settings.put(StringKeywords.aggLevel, 0);
            store.scan(settings, agg);
        } else {

            long       resMs      = 1000L * 60L * res;
            BucketInfo bucketInfo = new BucketInfo(t, resMs);

            // can the head be subdivided?
            if (bucketInfo.startTime != bucketInfo.roundedStartTime) {
                TimeRange thead = new TimeRange(bucketInfo.startTime, bucketInfo.roundedStartTime - 1L, t.window);
                _scan(settings, agg, thead, bucketsIdx + 1);
            }

            // did we find buckets for the body? If so, go ahead and scan
            if (t.endTime <= t.startTime) {
                TimeRange tbody = new TimeRange(bucketInfo.roundedStartTime, bucketInfo.roundedEndTime, t.window);
                timeSet.add(tbody);
                settings.put(StringKeywords.timeRangeSet, timeSet);
                settings.put(StringKeywords.aggLevel, res.intValue());
                store.scan(settings, agg);
            }

            // can the tail be subdivided?
            if (bucketInfo.roundedEndTime != bucketInfo.endTime) {
                TimeRange ttail = new TimeRange(bucketInfo.roundedEndTime + 1L, bucketInfo.endTime, t.window);
                _scan(settings, agg, ttail, bucketsIdx + 1);
            }
        }

    }

    @Override
    public void scan(HashMap<String, Object> settings, IAggregator agg) {
        HashSet<TimeRange> timeRangeSet = (HashSet<TimeRange>) settings.get(StringKeywords.timeRangeSet);

        for (TimeRange t : timeRangeSet) {
            if (t == null) {
                t = new TimeRange(0L, System.currentTimeMillis(), Window.ALL);
            }
            HashMap<String, Object> settingsCopy = new HashMap<>(settings);
            _scan(settingsCopy, agg, t, 0);
        }
    }

    @Override
    public void remove(HashMap<String, Object> settings) {
        store.remove(settings);
    }

    @Override
    public boolean populateValue(Metric metric) {
        return store.populateValue(metric);
    }
}
