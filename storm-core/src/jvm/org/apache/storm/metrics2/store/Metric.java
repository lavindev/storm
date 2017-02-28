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
import java.util.HashSet;
import java.util.Set;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Metric {
    private final static Logger LOG = LoggerFactory.getLogger(Metric.class);

    private String metricName;
    private String topoIdStr;
    private String host;
    private long port = 0;
    private String compIdStr;
    private long timestamp = 0;

    private Integer metricId;
    private Integer execId;
    private Integer compId;
    private Integer topoId;
    private Integer streamId;
    private Integer hostId;

    private String executor;
    private String dimensions;
    private String stream;

    private long count = 1L;
    private double value = 0.0;
    private double sum = 0.0;
    private double min = 0.0;
    private double max = 0.0;
    private Byte aggLevel = (byte)0;

    private static String[] prefixOrder = {
        StringKeywords.timeStart,
        StringKeywords.topoId, 
        StringKeywords.aggLevel,
        StringKeywords.metricSet,
        StringKeywords.component, 
        StringKeywords.executor, 
        StringKeywords.host,
        StringKeywords.port, 
        StringKeywords.stream
    };

    public Double getValue() {
        if (this.aggLevel == 0){
            return this.value; 
        } else {
            return this.sum;
        }
    }

    public void setAggLevel(Byte aggLevel){
        this.aggLevel = aggLevel;
    }

    public Byte getAggLevel(){
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
        LOG.debug("updating average {} {} {} {} {}", count, min, max, sum, value);
    }

    public Metric(String metric, Long timestamp, String executor, String compId, 
                  String stream, String topoIdStr, Double value) {
        this.metricName = metric;
        this.timestamp = timestamp;
        this.executor = executor;
        this.compIdStr = compId;
        this.topoIdStr = topoIdStr;
        this.stream = stream;
        this.value = value;

        // get numeric representation of each string property
        this.metricId = Metadata.getMetricId(metricName);
        this.execId = Metadata.getExecId(executor);
        this.compId = Metadata.getCompId(compId);
        this.topoId = Metadata.getTopoId(topoIdStr);
        this.streamId = Metadata.getStreamId(stream);
        this.hostId = Metadata.getHostId("localhost");
        //LOG.info(Metadata.contents());
    }

    public Metric(byte[] bytes) {
        deserialize(bytes);
        //LOG.info("New metric from db. Metadata has {} ", Metadata.contents());
        this.metricName = Metadata.getMetric(this.metricId);
        this.executor = Metadata.getExec(this.execId);
        this.compIdStr = Metadata.getComp(this.compId);
        this.topoIdStr = Metadata.getTopo(this.topoId);
        this.stream = Metadata.getStream(this.streamId);
        this.host = Metadata.getHost(this.hostId);
    }

    public String getCompId() { return this.compIdStr; }

    public Long getTimeStamp() { return this.timestamp; }

    public void setTimeStamp(Long timestamp) { this.timestamp = timestamp; }

    public String getTopoId() { return this.topoIdStr; }

    public String getMetricName() { return this.metricName; }
    
    public String getExecutor(){ return this.executor; }

    public byte[] serialize() {
        ByteBuffer bb = ByteBuffer.allocate(320);
        bb.put((byte)1); // non metadata
        bb.put(this.aggLevel);
        bb.putInt(topoId);
        bb.putLong(this.timestamp);
        bb.putInt(metricId);
        bb.putInt(compId);
        bb.putInt(execId);
        bb.putInt(hostId);
        bb.putLong(port);
        bb.putInt(streamId);

        int length = bb.position();
        bb.position(0); //rewind
        byte[] result = new byte[length];
        bb.get(result, 0, length);
        return result;
    }

    public void deserialize(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);

        bb.get(); // type
        this.aggLevel = bb.get();
        this.topoId = bb.getInt();
        this.timestamp = bb.getLong();
        this.metricId = bb.getInt();
        this.compId = bb.getInt();
        this.execId = bb.getInt();
        this.hostId = bb.getInt();
        this.port = bb.getLong();
        this.streamId = bb.getInt();
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
        x.append(this.topoIdStr);
        x.append("|");
        x.append(aggLevel);
        x.append("|");
        x.append(this.metricName);
        x.append("|");
        x.append(this.compIdStr);
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

    public String id() {
        StringBuilder x = new StringBuilder();
        x.append(this.topoIdStr);
        x.append("|");
        x.append(aggLevel);
        x.append("|");
        x.append(this.metricName);
        x.append("|");
        x.append(this.compIdStr);
        x.append("|");
        x.append(this.executor);
        x.append("|");
        x.append(this.host);
        x.append("|");
        x.append(this.port);
        x.append("|");
        x.append(this.stream);
        return x.toString();
    }

    public static byte[] createPrefix(Map<String, Object> settings){

        String topoIdStr   = (String) settings.get(StringKeywords.topoId);
        HashSet<String> metricIds = (HashSet<String>) settings.get(StringKeywords.metricSet);
        String metricIdStr = null;
        if (metricIds != null && metricIds.size() == 1){
            metricIdStr = metricIds.iterator().next();
        }

        String compIdStr   = (String) settings.get(StringKeywords.component);
        String execIdStr   = (String) settings.get(StringKeywords.executor);
        String host        = (String) settings.get(StringKeywords.host);
        String port        = (String) settings.get(StringKeywords.port);
        String stream      = (String) settings.get(StringKeywords.stream);
        Byte aggLevel      = ((Integer) settings.get(StringKeywords.aggLevel)).byteValue();

        LOG.info("Creating prefix for {} {} {} {} {} {} {} {}\n Meta: \n{}", 
                aggLevel, topoIdStr, metricIdStr, compIdStr, execIdStr, host, port, stream, Metadata.contents());


        ByteBuffer bb = ByteBuffer.allocate(320);
        bb.put((byte)1); // non metadata
        bb.put(aggLevel);
        Set<TimeRange> timeRangeSet = (Set<TimeRange>)settings.get(StringKeywords.timeRangeSet);
        Long cur = 0L;
        if (timeRangeSet != null){
            Long minTime = null;
            for (TimeRange tr : timeRangeSet){
                if (minTime == null || tr.startTime < minTime){
                    minTime = tr.startTime;
                }
            }
            cur = minTime;
        }
        LOG.info("The start time for prefix is {}, the topo is {}", cur, topoIdStr);
        bb.putInt(Metadata.getTopoId(topoIdStr));
        bb.putLong(cur == null ? 0L : cur.longValue());
        bb.putInt(metricIdStr != null ? Metadata.getMetricId(metricIdStr) : 0);
        bb.putInt(compIdStr   != null ? Metadata.getCompId(compIdStr) : 0);
        bb.putInt(execIdStr   != null ? Metadata.getExecId(execIdStr) : 0);
        bb.putInt(host        != null ? Metadata.getHostId(host) : 0);
        bb.putLong(port       != null ? Long.parseLong(port) : 0L);
        bb.putInt(stream      != null ? Metadata.getStreamId(stream) : 0);

        int length= bb.position();
        bb.position(0); // go to beginning
        byte[] result = new byte[length];
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
