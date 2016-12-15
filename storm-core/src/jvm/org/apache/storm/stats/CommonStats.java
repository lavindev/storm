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
package org.apache.storm.stats;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.internal.MultiCountStatAndMetric;
import org.apache.storm.metric.internal.MultiLatencyStatAndMetric;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.metrics2.SimpleGauge;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unchecked")
public abstract class CommonStats {
    public static final int NUM_STAT_BUCKETS = 20;

    public static final String RATE = "rate";

    public static final String EMITTED = "emitted";
    public static final String TRANSFERRED = "transferred";
    public static final String[] COMMON_FIELDS = {EMITTED, TRANSFERRED};

    protected final String executorIdStr;

    protected final int rate;
    protected final Map<String, IMetric> metricMap = new HashMap<>();

    protected final Map<String, Meter> codaHaleMetricMap = new HashMap<>();

    protected StormMetricRegistry metrics;

    public CommonStats(List<Long> executorId, StormMetricRegistry metrics, int rate) {
        this.rate = rate;
        this.executorIdStr = "[" + executorId.get(0) + "-" + executorId.get(1) + "]";
        this.put(EMITTED, new MultiCountStatAndMetric(NUM_STAT_BUCKETS));
        this.put(TRANSFERRED, new MultiCountStatAndMetric(NUM_STAT_BUCKETS));
        this.metrics = metrics;
    }

    public int getRate() {
        return this.rate;
    }

    public MultiCountStatAndMetric getEmitted() {
        return (MultiCountStatAndMetric) get(EMITTED);
    }

    public MultiCountStatAndMetric getTransferred() {
        return (MultiCountStatAndMetric) get(TRANSFERRED);
    }

    public IMetric get(String field) {
        return (IMetric) StatsUtil.getByKey(metricMap, field);
    }

    protected void put(String field, Object value) {
        StatsUtil.putKV(metricMap, field, value);
    }

    public void emittedTuple(String stream, String component) {
        this.getEmitted().incBy(stream, this.rate);
        this.getCounter(component, stream, EMITTED).inc(this.rate);
    }
   
    protected Counter getCounter(String component, String stream, String metricName){
        String fqMeterName = this.metrics.scopedName(executorIdStr, stream, component, metricName);
        Counter result = this.metrics.getCounters().get(fqMeterName);
        if (result == null) {
            return this.metrics.counter(fqMeterName);
        }
        return result;
    }

    protected Timer getTimer(String component, String stream, String metricName){
        String fqMeterName = this.metrics.scopedName(this.executorIdStr, stream, component, metricName);
        Timer result = this.metrics.getTimers().get(fqMeterName);
        if (result == null) {
            return this.metrics.timer(fqMeterName);
        }
        return result;
    }

    protected SimpleGauge<Long> getGauge(String component, String stream, String metricName){
        String fqMeterName = this.metrics.scopedName(this.executorIdStr, stream, component, metricName);
        SimpleGauge<Long> result = (SimpleGauge<Long>)this.metrics.getGauges().get(fqMeterName);
        if (result == null) {
            SimpleGauge<Long> simpleGauge = new SimpleGauge<Long>(0L);
            return this.metrics.register(fqMeterName, simpleGauge);
        }
        return result;
    }

    protected Histogram getHistogram(String component, String stream, String metricName){
        String fqMeterName = this.metrics.scopedName(this.executorIdStr, stream, component, metricName);
        Histogram result = this.metrics.getHistograms().get(fqMeterName);
        if (result == null) {
            Reservoir reservoir = new SlidingTimeWindowReservoir(1, TimeUnit.MINUTES);
            result = new Histogram(reservoir);
            return this.metrics.register(fqMeterName, result);
        }
        return result;
    }

    public void transferredTuples(String stream, String component, int amount) {
        this.getTransferred().incBy(stream, this.rate * amount);
        this.getCounter(component, stream, TRANSFERRED).inc(this.rate * amount);
    }

    public void cleanupStats() {
        for (Object imetric : this.metricMap.values()) {
            cleanupStat((IMetric) imetric);
        }
    }

    private void cleanupStat(IMetric metric) {
        if (metric instanceof MultiCountStatAndMetric) {
            ((MultiCountStatAndMetric) metric).close();
        } else if (metric instanceof MultiLatencyStatAndMetric) {
            ((MultiLatencyStatAndMetric) metric).close();
        }
    }

    protected Map valueStats(String[] fields) {
        Map ret = new HashMap();
        for (String field : fields) {
            IMetric metric = this.get(field);
            if (metric instanceof MultiCountStatAndMetric) {
                StatsUtil.putKV(ret, field, ((MultiCountStatAndMetric) metric).getTimeCounts());
            } else if (metric instanceof MultiLatencyStatAndMetric) {
                StatsUtil.putKV(ret, field, ((MultiLatencyStatAndMetric) metric).getTimeLatAvg());
            }
        }
        StatsUtil.putKV(ret, CommonStats.RATE, this.getRate());

        return ret;
    }

    protected Map valueStat(String field) {
        IMetric metric = this.get(field);
        if (metric instanceof MultiCountStatAndMetric) {
            return ((MultiCountStatAndMetric) metric).getTimeCounts();
        } else if (metric instanceof MultiLatencyStatAndMetric) {
            return ((MultiLatencyStatAndMetric) metric).getTimeLatAvg();
        }
        return null;
    }

    public abstract ExecutorStats renderStats();

}
