package org.apache.storm.metrics2.store;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;

public class MetricResult {
    public double value;
    public int count;
    private Map<TimeRange, Double> values;
    private Map<TimeRange, Long> counts;

    public MetricResult(){
        values = new HashMap<TimeRange, Double>();
        counts = new HashMap<TimeRange, Long>();
    }

    public void setValueFor(TimeRange tr, Double value){
        values.put(tr, value);
    }

    public Double getValueFor(TimeRange tr) {
        return values.get(tr);
    }

    public void incCountFor(TimeRange tr){
        Long count = counts.get(tr);
        count = count == null ? 0L : count;
        counts.put(tr, count++);
    }

    public Long getCountFor(TimeRange tr){
        return counts.get(tr);
    }

    public Set<TimeRange> getTimeRanges(){
        return values.keySet();
    }
}
