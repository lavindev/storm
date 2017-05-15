package org.apache.storm.metrics2.store;

import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//TODO: this should be an internal enum
import org.apache.storm.generated.Window;

public class AggregatingMetricStore implements MetricStore {
    private final static Logger LOG = LoggerFactory.getLogger(AggregatingMetricStore.class);
    private MetricStore store;

    private List<Integer> _buckets;

    public AggregatingMetricStore(MetricStore store){
        this.store = store;
        _buckets = new ArrayList<Integer>();
        _buckets.add(60); // 60 minutes
        _buckets.add(10); // 10 minutes
        _buckets.add(1);  // 1 minutes
    }

    @Override
    public void prepare(Map config){
        // For now, nothing to configure
    }

    @Override 
    public void insert(Metric metric){
        LOG.debug("Inserting {}", metric);
        store.insert(metric);

        // transform metric to insert in aggregating buckets

        // incoming value is instantaneous
        Double value = metric.getValue();
        Long timeInMs = metric.getTimeStamp();
       
        // foreach bucket, pull the existing aggregation and update it with
        // the new value
        for (Integer bucket : _buckets) {
            // get the bucket in millis
            long msToBucket = 1000 * bucket * 60;

            // the time for which this metric value applies to
            Long roundedToBucket = msToBucket * (long)Math.floor((double)timeInMs / msToBucket);
            metric.setTimeStamp(roundedToBucket);

            metric.setAggLevel(bucket.byteValue());

            // using the metric reference, populate it with what was in store for the bucket
            store.populateValue(metric); 

            // then apply the new value
            metric.updateAverage(value);
            LOG.debug("inserting {} min bucket {}", bucket, metric.toString());
            store.insert(metric);
        }
    }

    @Override 
    public void scan(IAggregator agg){
        //TODO: implement or remove
        store.scan(agg);
    }

    public Long[] bucketize(TimeRange t, int resolution) {
        // we go until endTime. If endTime is null, we go until now
        boolean truncateTail = t.endTime != null;
        long endTime = t.endTime == null ? System.currentTimeMillis() : t.endTime;
        long roundedEndTime = resolution * (endTime/ resolution);
        long endTimeAtResolution = roundedEndTime + resolution; // end of bucket
        
        // endTimeAtResolution, t.endTime are the start and stop for tail end
        long tail = endTime - endTimeAtResolution;
        if (tail < 0 && truncateTail) {
            // endtime is within the bucket
            // but we don't want that, we'd rather use the higher resultion values there
            endTimeAtResolution = roundedEndTime;
            tail = t.endTime - endTimeAtResolution;
        }

        long roundedStartTime = resolution * (long)Math.floor((double)t.startTime/ resolution);
        long N = roundedEndTime - roundedStartTime;

        // t.startTime, roundedStartTime the start and stop for head end
        long head = roundedStartTime - t.startTime;

        Date startTime= new Date(t.startTime);
        Date endTimeDate= t.endTime != null? new Date(t.endTime) : null;
        Date roundedEndTimeDate = new Date(roundedEndTime);
        Date roundedStartTimeDate = new Date(roundedStartTime);

        DateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        //TODO: simplify this
        LOG.info ("num buckets {}, resolution (ms) {}, start time {}, end time {} end time (rounded) {}\n" +
                  "head ms {} head start {} head end {}\n tail ms {} tail start {} tail end {}", 
                N/resolution, resolution, 
                format.format(startTime), 
                endTimeDate != null ? format.format(endTimeDate) : null, 
                format.format(roundedEndTimeDate),
                head, 
                format.format(startTime), 
                format.format(roundedStartTimeDate),
                tail, 
                endTimeAtResolution, 
                endTimeDate != null ? format.format(endTimeDate) : null);
        return new Long[] {
            t.startTime,
            roundedStartTime,
            endTimeAtResolution,
            t.endTime
        };
    }

    int getFiner(int res){
        if (res == 60) {
            return 10;
        } else if (res == 10) {
            return 1;
        }
        return 0;
    }

    public void _scan(HashMap<String, Object> settings, IAggregator agg, TimeRange t, int res) {
        LOG.info ("At _scan buckets with {} {} {} {}", settings, agg, t, res);

        if (res == 0) {
            // raw, just go ahead and scan
            HashSet<TimeRange> timeSet = new HashSet<TimeRange>();
            timeSet.add(t);
            settings.put(StringKeywords.timeRangeSet, timeSet);
            settings.put("aggLevel", 0);
            LOG.info("scan remainder bucket {}", settings);
            store.scan(settings, agg);
        } else {
            int nextRes = getFiner(res);
            int msToRes = 1000 * 60 * res;
            Long[] buckets = bucketize(t, msToRes);

            // can the head be subdivided?
            if (buckets[0] != buckets[1]) {
                TimeRange thead = new TimeRange(buckets[0], buckets[1], t.window);
                LOG.info("getting buckets for head {} {}", buckets[0], buckets[1]);
                _scan(settings, agg, thead, nextRes);
            } else {
                LOG.info("no need to scan head bucket");
            }

            // did we find buckets for the body? If so, go ahead and scan
            TimeRange tbody = new TimeRange(buckets[1], buckets[2], t.window);
            HashSet<TimeRange> timeSet = new HashSet<TimeRange>();
            timeSet.add(tbody);
            settings.put(StringKeywords.timeRangeSet, timeSet);
            settings.put(StringKeywords.aggLevel, ((Integer)res).intValue());
            LOG.info("scan body bucket {} settings: {}", tbody, settings);
            store.scan(settings, agg);

            // can the tail be subdivided?
            if (buckets[2] != buckets[3]) {
                TimeRange ttail = new TimeRange(buckets[2], buckets[3], t.window);
                LOG.info("getting buckets for tail {} {}", buckets[2], buckets[3]);
                _scan(settings, agg, ttail, nextRes);
            } else {
                LOG.info("no need to scan tail bucket");
            }
        }
    }

    @Override 
    public void scan(HashMap<String, Object> settings, IAggregator agg){
        HashSet<TimeRange> timeRangeSet = (HashSet<TimeRange>)settings.get(StringKeywords.timeRangeSet);
        for (TimeRange t : timeRangeSet) {
            LOG.info("Checking time range {}", t);
            if (t == null){
                t = new TimeRange(0L, null, Window.ALL);
            }

            HashMap<String, Object> settingsCopy = new HashMap<String, Object>(settings);
            _scan(settingsCopy, agg, t, _buckets.get(0));
        }
    }

    @Override 
    public void remove(HashMap<String, Object> settings){
        store.remove(settings);
    }

    @Override
    public boolean populateValue(Metric metric){
        return store.populateValue(metric);
    }
}
