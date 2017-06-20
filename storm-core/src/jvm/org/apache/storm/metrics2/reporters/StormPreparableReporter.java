/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.metrics2.reporters;

import java.util.concurrent.TimeUnit;
import java.util.SortedMap;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.MetricRegistryListener;

import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.LSWorkerStats;

import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.daemon.metrics.MetricsUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: move out of reporters namespace
import org.apache.storm.metrics2.reporters.PreparableReporter;

public class StormPreparableReporter implements PreparableReporter {

    ScheduledReporter scheduledReporter;

    private static final Logger LOG = LoggerFactory.getLogger(StormPreparableReporter.class);

    public StormPreparableReporter() {
    }

    @Override
    public void prepare(MetricRegistry registry, Map stormConf, Map reporterConf, String daemonId) {
        try {
            LOG.info("Configuring StormPreparableReporter for {}: {}", daemonId, reporterConf);
            LocalState state = ConfigUtils.workerState (stormConf, daemonId);
            this.scheduledReporter = new StormMetricScheduledReporter(registry, state, MetricFilter.ALL, TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS);
            TimeUnit unit = MetricsUtils.getMetricsSchedulePeriodUnit(reporterConf);
            long interval = MetricsUtils.getMetricsSchedulePeriod(reporterConf);
            this.scheduledReporter.start(interval, unit);
        } catch (Exception e){
            LOG.error ("Exception preparing the StormPreparableReporter", e);
        }
    }

    @Override
    public void start(){
    }

    @Override
    public void stop(){
    }

    class StormMetricScheduledReporter extends ScheduledReporter {
        LocalState state;

        long _reportTime = 0;
        long _prevReportTime = 0;

        private ConcurrentMap<String, Long> counterCache;

        public StormMetricScheduledReporter (MetricRegistry registry, LocalState state, 
                MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit) {
            super(registry, "storm-default-reporter", filter, rateUnit, durationUnit);

            counterCache = new ConcurrentHashMap<String, Long>();

            // compute cache
            registry.addListener(new MetricRegistryListener() {
                @Override
                public void onCounterRemoved(String name){
                    LOG.info("onCounterRemoved {}", name);
                    //counterCache.remove(name);
                }

                @Override
                public void onCounterAdded(String name, Counter counter){
                    //counterCache.put(name, new Long(0)); // always start this at 0, the report function updates
                    LOG.info("onCounterAdded {}", name);
                }

                @Override
                public void onGaugeAdded(String name, Gauge<?> gauge) {
                    LOG.info("onGaugeAdded {}", name);
                }

                @Override
                public void onGaugeRemoved(String name) {
                    LOG.info("onGaugeRemoved {}", name);
                }

                @Override
                public void onHistogramAdded(String name, Histogram hist) {}

                @Override
                public void onHistogramRemoved(String name) {}

                @Override
                public void onMeterAdded(String name, Meter meter) {}

                @Override
                public void onMeterRemoved(String name) {}

                @Override
                public void onTimerAdded(String name, Timer timer) {}

                @Override
                public void onTimerRemoved(String name) {}
            });

            this.state = state;
        }

        @Override
        public void report(SortedMap<String,Gauge> gauges, 
                    SortedMap<String,Counter> counters, 
                    SortedMap<String,Histogram> histograms, 
                    SortedMap<String,Meter> meters, 
                    SortedMap<String,Timer> timers){

            _prevReportTime = _reportTime;
            _reportTime = System.currentTimeMillis();

            LOG.info("Got call to report at {} ({} previous) -- with gauges: {} counters: {} histograms: {} meters: {} timers: {}",
                    _reportTime, _prevReportTime,
                    gauges == null   ? "null" : gauges.size(),
                    counters == null ? "null" : counters.size(),
                    histograms == null ? "null" : histograms.size(),
                    meters == null ? "null" : meters.size(),
                    timers == null ? "null" : timers.size());
           
            LSWorkerStats workerStats = new LSWorkerStats(); 
            if (counters != null) {
                for (Map.Entry<String, Counter> c : counters.entrySet()) {
                    String key = c.getKey();
                    long count = c.getValue().getCount();
                    putDelta(workerStats, key, count);
                }
            }
            if (gauges != null) {
                for (Map.Entry<String, Gauge> c : gauges.entrySet()) {
                    String key = c.getKey();
                    LOG.info("Gauge {}", key);
                    // TODO: value here is an object, so we are making all kinds of assumptions
                    double value = ((Number)c.getValue().getValue()).doubleValue();
                    LOG.info("Gauge k {} v {}", key, value);
                    workerStats.put_to_metrics(key, value);
                }
            }
            if (meters != null) {
                for (Map.Entry<String, Meter> m : meters.entrySet()) {
                    String key = m.getKey();
                    LOG.info("Meter {}", key);
                    long value = (Long)m.getValue().getCount();
                    putDelta(workerStats, key, value);
                }
            }
            if (histograms != null) {
                LOG.warn("Histograms are not supported by the StormPreparableReporter");
            }
            if (timers != null) {
                LOG.warn("Timers are not supported by the StormPreparableReporter");
            }
            state.setWorkerStats(workerStats);
        }

        private void putDelta(LSWorkerStats workerStats, String key, long count){
            // check the cache to see if the value changed
            Long oldCount = counterCache.get(key);
            oldCount = oldCount == null ? 0L : oldCount;

            // report the delta between the old count and the new count
            // if count is positive, take diff
            // oldCount is always <= count (counters are always increasing)
            long reportCount = count > 0 ? count - oldCount : 0;
            LOG.info ("{}: would send {} current is {} old was {}", 
                    key, reportCount, count, oldCount);

            // for the next call
            counterCache.put(key, count);

            workerStats.set_time_stamp(_reportTime);
            workerStats.put_to_metrics(key, new Double(reportCount));
        }
    }
}