package org.apache.storm.metrics2.store;

//TODO: this should be an internal enum
import org.apache.storm.generated.Window;

public class TimeRange {
    public Long startTime;
    public Long endTime;
    public Window window;

    public TimeRange(Long startTime, Long endTime, Window window){
        this.startTime = startTime;
        this.endTime = endTime;
        this.window = window;
    }

    public TimeRange() {
        this.startTime = null;
        this.endTime = null;
        this.window = Window.ALL; //?
    }

    public boolean contains(Long time){
        if ((startTime == null || (startTime != null && time >= startTime)) && 
            (endTime == null || (endTime != null && time <= endTime))){
            return true;
        }
        return startTime == null && endTime == null;
    }

    public String toString(){
        return "start: " + this.startTime + " end: " + this.endTime + " window: " + this.window;
    }
}
