/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.metrics2.store;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class HBaseBench implements Runnable {
    private final static Logger LOG = LoggerFactory.getLogger(HBaseBench.class);

    private int writeCount;
    private int incrementalRuns;
    private List<String> metricNames;
    private long threadID;
    private long prefix;
    private Random random;
    private HBaseStore store;
    private Thread t;

    private ArrayList<Integer> writeTimer;
    private ArrayList<Integer> scanTimer;
    private ArrayList<Integer> aggWriteTimer;

    HBaseBench(HBaseStore store, long threadID) {
        this.writeCount = 20;
        this.incrementalRuns = 10;
        this.metricNames = Arrays.asList("testMetric1", "testMetric2", "testMetric3");
        this.threadID = threadID;
        this.random = new Random();
        this.prefix = random.nextLong();
        this.store = store;
        this.writeTimer = new ArrayList<>();
        this.aggWriteTimer = new ArrayList<>();
        this.scanTimer = new ArrayList<>();
    }

    HBaseBench withMetricNames(List<String> metrics) {
        this.metricNames = metrics;
        return this;
    }

    private long timeNow() {
        return System.currentTimeMillis();
    }

    private Metric makeMetric(String metricName) {
        long ts = System.currentTimeMillis();
        Metric m = new Metric(metricName, ts,
                prefix + "Executor",
                prefix + "Comp",
                prefix + "Stream" + random.nextInt(),
                prefix + "Topo",
                123.45);
        m.setHost("testHost");
        m.setAggLevel((byte) 1); // for consistency during scan
        return m;
    }

    private Metric makeAggMetric(String metricName) {
        Metric m = makeMetric(metricName);
        m.setValue(100.0);
        for (int i = 1; i < 10; ++i) {
            m.updateAverage(100.00 * i);
        }
        return m;
    }

    public void start() {
        LOG.info("Running for node with id = {}", threadID);

        if (t == null) {
            t = new Thread(this, String.valueOf(threadID));
            t.start();
        } else {
            LOG.info("thread already running");
        }

    }


    public void run() {
        LOG.info("run() for id={}", threadID);
        for (int i = 0; i < this.incrementalRuns; ++i) {
            LOG.info("insert() for id={}", threadID);
            insert();
            LOG.info("scan() for id={}", threadID);
            scan();
        }
        LOG.info("Benchmark complete for {}", threadID);
        LOG.info("Write timer {} = {}", threadID, writeTimer);
        LOG.info("Aggregate Write timer {} = {}", threadID, aggWriteTimer);
        LOG.info("Scan timer {} = {}", threadID, scanTimer);
    }


    private void insert() {
        for (int i = 0; i <= writeCount; ++i) {
            long startTime = timeNow();
            for (String metricName : metricNames) {
                Metric m = makeMetric(metricName);
                store.insert(m);
            }
            long endTime  = timeNow();
            int  duration = (int) (endTime - startTime);
            writeTimer.add(duration);
        }

        for (int i = 0; i <= writeCount; ++i) {
            long startTime = timeNow();
            for (String metricName : metricNames) {
                Metric m = makeAggMetric(metricName);
                store.insert(m);
            }
            long endTime  = timeNow();
            int  duration = (int) (endTime - startTime);
            aggWriteTimer.add(duration);
        }
    }

    private void scan() {
        int[] counter = new int[1];
        counter[0] = 0;
        HashMap<String, Object> settings = new HashMap<>();
        settings.put(StringKeywords.aggLevel, 1);
        settings.put(StringKeywords.topoId, String.valueOf(prefix) + "Topo");

        long startTime = timeNow();
        store.scan(settings, (metric, timeRange) -> ++counter[0]);
        long endTime  = timeNow();
        int  duration = (int) (endTime - startTime);
        scanTimer.add(duration);
        LOG.info("Scanned {} metrics", counter[0]);
    }


}
