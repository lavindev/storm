package org.apache.storm.metrics2;

import com.codahale.metrics.Gauge;

public class SimpleGauge<T> implements Gauge<T> {
    private T value;

    public SimpleGauge(T value){
        this.value = value;
    }

    @Override
    public T getValue() {
        return this.value;
    }

    public void update(T value){
        this.value = value;
    }
}
