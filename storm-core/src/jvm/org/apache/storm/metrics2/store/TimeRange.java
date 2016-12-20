package org.apache.storm.metrics2.store;

public class TimeRange {
    public Long startTime;
    public Long endTime;

    public TimeRange(Long startTime, Long endTime){
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public TimeRange() {
        this.startTime = null;
        this.endTime = null;
    }

    public boolean contains(Long time){
        if ((startTime != null && time >= startTime) ||
            (endTime != null && time <= endTime)){
            return true;
        }
        return startTime == null && endTime == null;
    }
}
