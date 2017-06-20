package org.apache.storm.metrics2.store;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricResult {
    private final static Logger LOG = LoggerFactory.getLogger(MetricResult.class);
    private Map<String, Map<TimeRange, Double>> values;
    private Map<String, Map<TimeRange, Long>> counts;

    public MetricResult(){
        values = new HashMap<String, Map<TimeRange, Double>>();
        counts = new HashMap<String, Map<TimeRange, Long>>();
    }

    public void setValueFor(String metricName, TimeRange tr, Double value){
        Map<TimeRange, Double> metricMap = values.get(metricName);
        if (metricMap == null) {
            metricMap = new HashMap<TimeRange, Double>();
            values.put(metricName, metricMap);
        }
        metricMap.put(tr, value);
    }

    public Double getValueFor(String metricName, TimeRange tr) {
        Map<TimeRange, Double> metricMap = values.get(metricName);
        if (metricMap == null) {
            return null;
        }
        return metricMap.get(tr);
    }

    public void incCountFor(String metricName, TimeRange tr) {
        incCountFor(metricName, tr, 1);
    }
    public void incCountFor(String metricName, TimeRange tr, long incBy){
        LOG.info("incCountFor {} {}", metricName, tr);
        Map<TimeRange, Long> countMap = counts.get(metricName);
        LOG.info("incCountFor countMap {} {} is {}", metricName, tr, countMap);
        if (countMap == null) {
            countMap = new HashMap<TimeRange, Long>();
            counts.put(metricName, countMap);
        }
        Long count = countMap.get(tr);
        count = count == null ? 0L : count;
        countMap.put(tr, count + incBy);
    }

    public Long getCountFor(String metricName, TimeRange tr){
        LOG.info("getCountFor {} {}", metricName, tr);
        Map<TimeRange, Long> countMap = counts.get(metricName);
        LOG.info("getCountFor countMap {} {} is {}", metricName, tr, countMap);
        if (countMap == null) {
            return null;
        }
        return countMap.get(tr);
    }

    public Set<TimeRange> getTimeRanges(String metricName){
        Map<TimeRange, Double> metricMap = values.get(metricName);
        if (metricMap == null) {
            return null;
        }
        return metricMap.keySet();
    }

    public Set<String> getMetricNames(){
        return values.keySet();
    }
}