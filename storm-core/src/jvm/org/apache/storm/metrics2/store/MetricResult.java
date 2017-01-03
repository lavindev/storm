package org.apache.storm.metrics2.store;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;

public class MetricResult {
    public double value;
    public int count;
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

    public void incCountFor(String metricName, TimeRange tr){
        Map<TimeRange, Long> countMap = counts.get(metricName);
        if (countMap == null) {
            countMap = new HashMap<TimeRange, Long>();
            counts.put(metricName, countMap);
        }
        Long count = countMap.get(tr);
        count = count == null ? 0L : count;
        countMap.put(tr, count++);
    }

    public Long getCountFor(String metricName, TimeRange tr){
        Map<TimeRange, Long> countMap = counts.get(metricName);
        if (countMap == null) {
            return null;
        }
        return countMap.get(metricName);
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
