/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.metrics2.store;

import java.lang.String;
import java.lang.StringBuilder;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Metric {
    private final static Logger LOG = LoggerFactory.getLogger(Metric.class);

    private String metricName;
    private String topoId;
    private String host;
    private long port = 0;
    private String compId;
    private long timestamp = 0;

    private String executor;
    private String dimensions;
    private String stream;

    private long count = 1L;
    private double value = 0.0;
    private double sum = 0.0;
    private double min = 0.0;
    private double max = 0.0;
    private String aggLevel = "rt";
    private static String[] prefixOrder = {
        StringKeywords.timeStart
    //   StringKeywords.topoId, 
    //   StringKeywords.aggLevel,
    //   StringKeywords.metricName
    };/*
        StringKeywords.component, 
        StringKeywords.executor, 
        StringKeywords.host,
        StringKeywords.port, 
        StringKeywords.stream
    };
    */

    public Double getValue() {
        if (this.aggLevel.equals("rt")){
            return this.value; 
        } else {
            return this.sum;
        }
    }

    public void setAggLevel(String aggLevel){
        this.aggLevel = aggLevel;
    }

    public String getAggLevel(){
        return this.aggLevel;
    }

    public void setValue(Double value) {
        this.count = 1L;
        this.min = value;
        this.max = value;
        this.sum = value;
        this.value = value;
    }

    public void updateAverage(Double value) {
        this.count += 1;
        this.min = Math.min(this.min, value);
        this.max = Math.max(this.max, value);
        this.sum += value;
        this.value = this.sum / this.count;
        LOG.info("updating average {} {} {} {} {}", count, min, max, sum, value);
    }

    public Metric(String metric, Long timestamp, String executor, String compId, 
                  String stream, String topoId, Double value) {
        this.metricName = metric;
        this.timestamp = timestamp;
        this.executor = executor;
        this.compId = compId;
        this.topoId = topoId;
        this.stream = stream;
        this.value = value;
    }

    public Metric(byte[] bytes) {
        deserialize(bytes);
    }

    public String getCompId() { return this.compId; }

    public Long getTimeStamp() { return this.timestamp; }

    public void setTimeStamp(Long timestamp) { this.timestamp = timestamp; }

    public String getTopoId() { return this.topoId; }

    public String getMetricName() { return this.metricName; }

    public byte[] serialize() {
        ByteBuffer bb = ByteBuffer.allocate(320);
        bb.putLong(this.timestamp);
        putString(bb, this.topoId);
        putString(bb, aggLevel);
        putString(bb, metricName);
        putString(bb, compId);
        putString(bb, executor);
        putString(bb, host);
        bb.putLong(port);
        putString(bb, stream);
        int length = bb.position();
        bb.position(0); //rewind
        byte[] result = new byte[length];
        bb.get(result, 0, length);
        return result;
    }

    public void deserialize(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        this.timestamp = bb.getLong();
        this.topoId = getString(bb);
        this.aggLevel = getString(bb);
        this.metricName = getString(bb);
        this.compId = getString(bb);
        this.executor = getString(bb);
        this.host = getString(bb);
        this.port = bb.getLong();
        this.stream = getString(bb);
    }

    public static void putString(ByteBuffer bb, String string) {
        if (string == null) {
            bb.putInt(0);
            return;
        }
        int size = string.length();
        bb.putInt(size);
        try { 
            bb.put(string.getBytes("UTF-8"));
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public String getString(ByteBuffer bb) {
        int size = bb.getInt();
        if (size == 0){
            return null;
        }
        byte[] bytes = new byte[size];
        bb.get(bytes);
        try { 
            return new String(bytes, "UTF-8");
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }

    public String toString() {
        StringBuilder x = new StringBuilder();
        x.append(String.format("%015d",this.timestamp));
        x.append("|");
        x.append(this.topoId);
        x.append("|");
        x.append(aggLevel);
        x.append("|");
        x.append(this.metricName);
        x.append("|");
        x.append(this.compId);
        x.append("|");
        x.append(this.executor);
        x.append("|");
        x.append(this.host);
        x.append("|");
        x.append(this.port);
        x.append("|");
        x.append(this.stream);
        return x.toString() + " => count: " + this.count + " value: " + this.value + " min: " + this.min + " max: " + this.max + " sum: " + this.sum;
    }

    public static byte[] createPrefix(Map<String, Object> settings){
        StringBuilder x = new StringBuilder();
        ByteBuffer bb = ByteBuffer.allocate(320);
        for(String each : prefixOrder) {
            Object cur = null;

            if (each == StringKeywords.timeStart){
                // find minimium time beteween all time ranges
                // this would be better organized as an ordered set
                Set<TimeRange> timeRangeSet = (Set<TimeRange>)settings.get(StringKeywords.timeRangeSet);
                if (timeRangeSet != null){
                    Long minTime = null;
                    for (TimeRange tr : timeRangeSet){
                        if (minTime == null || tr.startTime < minTime){
                            minTime = tr.startTime;
                        }
                    }
                    cur = minTime;
                }
                LOG.info("The start time for prefix is {}", cur);
                bb.putLong(cur == null ? 0L : (long)cur);
            } else {
                cur = settings.get(each);
                if (cur == null && each == StringKeywords.aggLevel){
                    cur = "rt";
                }
                putString(bb, cur == null ? null : (String)cur);
            }

            if(cur == null){
                break;
            }
        }   
        int length= bb.position();
        byte[] result = new byte[length];
        bb.position(0); // go to beginning
        bb.get(result, 0, length); // copy to position
        return result;
    }

    public byte[] getKeyBytes(){
        return this.serialize();
    }

    public byte[] getValueBytes(){
        int bufferSize = count > 1 ? 320 : 128;
        ByteBuffer bb = ByteBuffer.allocate(bufferSize);
        bb.putLong(count);
        bb.putDouble(value);
        bb.putDouble(min);
        bb.putDouble(max);
        bb.putDouble(sum);

        int length = bb.position();
        bb.position(0); //rewind
        byte[] result = new byte[length];
        bb.get(result, 0, length);
        return result;
    }

    public void setValueFromBytes(byte[] valueInBytes){
        if (valueInBytes == null) {
            LOG.error("Null bytes!");
            count = 0L;
            value = 0.0;
            min = 0.0;
            max = 0.0;
            sum = 0.0;
            return;
        }
        ByteBuffer bb = ByteBuffer.wrap(valueInBytes);
        count = bb.getLong();
        value = bb.getDouble();
        min = bb.getDouble();
        max = bb.getDouble();
        sum = bb.getDouble();
    }

    public static byte[] longToBytes(long l) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte)(l & 0xFF);
            l >>= 8;
        }
        return result;
    }

    public static long bytesToLong(byte[] b) {
        long result = 0;
        for (int i = 0; i < 8; i++) {
            result <<= 8;
            result |= (b[i] & 0xFF);
        }
        return result;
    }

}
