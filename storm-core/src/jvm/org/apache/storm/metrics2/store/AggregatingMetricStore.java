package org.apache.storm.metrics2.store;

import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.List;

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
    }

    @Override
    public void prepare(Map config){
        //TODO: I don't think this isgetting called
        if (config.containsKey("storm.metrics2.store.aggregation_buckets")) {
            _buckets = (List<Integer>)config.get("storm.metrics2.store.aggregation_buckets");
        }
    }

    @Override 
    public void insert(Metric metric){
        // value is instantaneous
        Double value = metric.getValue();
        
        for (Integer bucket : _buckets) {
            LOG.debug("before inserting {} min bucket {} val: {}", bucket, metric.toString(), value);
            // get the hourly bucket for metric
            Long timeInMs = metric.getTimeStamp();
            long msToBucket = 1000 * bucket * 60;
            Long roundedToBucket = msToBucket * (timeInMs / msToBucket);
            metric.setTimeStamp(roundedToBucket);
            metric.setAggLevel(bucket.byteValue());

            store.populateValue(metric); 
            LOG.debug("after populating {} min bucket {} val: {}", bucket, metric.toString(), value);
            metric.updateAverage(value);
            LOG.debug("inserting {} min bucket {}", bucket, metric.toString());
            store.insert(metric);
        }
    }

    @Override 
    public void scan(IAggregator agg){
        store.scan(agg);
    }

    public Long[] bucketize(TimeRange t, long resolution) {
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

        //TODO: RHS of this used to be a Math.ceil
        long roundedStartTime = resolution * (long)Math.ceil((double)t.startTime/ resolution);
        long N = roundedEndTime - roundedStartTime;

        // t.startTime, roundedStartTime the start and stop for head end
        long head = roundedStartTime - t.startTime;

        LOG.info ("num buckets {}, resolution (ms) {}, start time {}, end time {} end time (rounded) {}\n" +
                  "head ms {} head start {} head end {}\n tail ms {} tail start {} tail end {}", 
                N/resolution, resolution, t.startTime, t.endTime, roundedEndTime,
                head, t.startTime, roundedStartTime,
                tail, endTimeAtResolution, t.endTime);
        return new Long[] {
            t.startTime,
            roundedStartTime,
            endTimeAtResolution,
            t.endTime
        };
    }

    long getFiner(long res){
        if (res == 60L) {
            return 0L;
        }
        return 0L;
        //   //    return 10L;
        //   //case 10L:
        //   //    return 1L;
        //   default:
        //       return 0L;
        //
    }

    public void _scan(HashMap<String, Object> settings, IAggregator agg, TimeRange t, long res) {
        LOG.info ("At _scan buckets with {} {} {} {}", settings, agg, t, res);
        HashSet<TimeRange> timeSet = new HashSet<TimeRange>();
        settings.put(StringKeywords.timeRangeSet, timeSet);
        if (res == 0) {
            // raw, just go ahead and scan
            timeSet.add(t);
            settings.put("aggLevel", 0);
            LOG.info("scan remainder bucket {}", settings);
            store.scan(settings, agg);
        } else {
            long nextRes = getFiner(res);
            long msToRes = 1000 * 60 * res;
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
            timeSet.clear();
            timeSet.add(tbody);
            settings.put("aggLevel", ((Long)res).intValue());
            settings.put(StringKeywords.timeRangeSet, timeSet);
            LOG.info("scan body bucket {} settings: {}", tbody, settings);
            store.scan(settings, agg);

            // can the tail be subdivided?
            if (buckets[0] != buckets[1]) {
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
            _scan(settingsCopy, agg, t, 60);

           //long msToHour= 1000 * 60 * 60;
           //Long[] buckets = bucketize(t, msToHour);

           //// now that we have overall buckets with coarsest resolution, 
           //// try to get finer aggregations for the head and the tail

           //HashSet<TimeRange> timeSet = new HashSet<TimeRange>();

           //TimeRange thead = new TimeRange(buckets[0], buckets[1], t.window);

           //// try 10 minutes
           //long msTo10Minutes = 1000 * 60 * 10;
           //Long[] tenMinuteHeadBuckets = bucketize(thead, msTo10Minutes);

           //thead = new TimeRange(tenMinuteHeadBuckets[0], tenMinuteHeadBuckets[1], t.window);

           //long msToMinutes = 1000 * 60;
           //Long[] minuteHeadBuckets = bucketize(thead, msToMinutes);

           //timeSet.add(new TimeRange(buckets[0], buckets[1], t.window));
           //settingsCopy.put(StringKeywords.timeRangeSet, timeSet);

           //if (buckets[0] != buckets[1]){
           //    settingsCopy.put("aggLevel", 0);
           //    LOG.info("scan head rt {}", settingsCopy);
           //    store.scan(settingsCopy, agg);
           //}

           //timeSet.clear();
           //timeSet.add(new TimeRange(buckets[1], buckets[2], t.window));
           //settingsCopy.put("aggLevel", 60);
           //LOG.info("scan meat {}", settingsCopy);
           //store.scan(settingsCopy, agg);

           //if (buckets[2] != buckets[3]){
           //    timeSet.clear();
           //    timeSet.add(new TimeRange(buckets[2], buckets[3], t.window));
           //    settingsCopy.put("aggLevel", 0);
           //    LOG.info("scan tail rt {}", settingsCopy);
           //    store.scan(settingsCopy, agg);
           //}
        }
    }

    @Override 
    public void remove(HashMap<String, Object> settings){
        settings.put("aggLevel", 60);
        store.remove(settings);
    }

    @Override
    public boolean populateValue(Metric metric){
        metric.setAggLevel((byte)60);
        return store.populateValue(metric);
    }
}
