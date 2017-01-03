package org.apache.storm.metrics2.store;

import java.util.Map;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregatingMetricStore implements MetricStore {
    private final static Logger LOG = LoggerFactory.getLogger(AggregatingMetricStore.class);
    private MetricStore store;
    private final long msToHour = 1000 * 3600;

    public AggregatingMetricStore(MetricStore store){
        this.store = store;
    }

    @Override
    public void prepare(Map config){
        // noop
    }

    @Override 
    public void insert(Metric metric){
        Double value = metric.getValue();
        LOG.info("before inserting hourly {} val: {}", metric.toString(), value);
        // get the hourly bucket for metric
        Long timeInMs = metric.getTimeStamp();
        Long roundedToHour = msToHour * (timeInMs / msToHour);
        metric.setTimeStamp(roundedToHour);
        metric.setAggLevel("hourly");

        store.populateValue(metric); 
        LOG.info("after populating hourly {} val: {}", metric.toString(), value);
        metric.updateAverage(value);
        LOG.info("inserting hourly {}", metric.toString());
        store.insert(metric);
    }

    @Override 
    public void scan(IAggregator agg){
        store.scan(agg);
    }

    @Override 
    public void scan(HashMap<String, Object> settings, IAggregator agg){
        LOG.info("scan hourly {}", settings);
        settings.put("aggLevel", "hourly");
        store.scan(settings, agg);
    }

    @Override 
    public void remove(HashMap<String, Object> settings){
        settings.put("aggLevel", "hourly");
        store.remove(settings);
    }

    @Override
    public boolean populateValue(Metric metric){
        metric.setAggLevel("hourly");
        return store.populateValue(metric);
    }
}
