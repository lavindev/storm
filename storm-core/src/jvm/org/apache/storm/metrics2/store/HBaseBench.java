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


import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class HBaseBench {
    private final static Logger LOG = LoggerFactory.getLogger(HBaseBench.class);

    private int scanCount;
    private int writeCount;
    private int rounds;
    private List<String> metricNames;
    private long idPrefix;
    private Random random;
    private HBaseStore store;

    private HashMap<Integer, ArrayList<Integer>> writeTimer;
    private HashMap<Integer, ArrayList<Integer>> scanTimer;
    private HashMap<Integer, ArrayList<Integer>> aggWriteTimer;

    public HBaseBench(HBaseStore store, long idPrefix) {
        this.scanCount = 25;
        this.writeCount = 500;
        this.rounds = 3;
        this.metricNames = Arrays.asList("testMetric1", "testMetric2", "testMetric3");
        this.idPrefix = idPrefix;
        this.random = new Random();
        this.store = store;
        this.writeTimer = new HashMap<Integer, ArrayList<Integer>>(rounds);
        this.aggWriteTimer = new HashMap<Integer, ArrayList<Integer>>(rounds);
        this.scanTimer = new HashMap<Integer, ArrayList<Integer>>(rounds);
    }

    public HBaseBench withMetricNames(List<String> metrics) {
        this.metricNames = metrics;
        return this;
    }

    private long timeNow() {
        return System.currentTimeMillis();
    }

    private Metric makeMetric(String metricName) {
        long ts = System.currentTimeMillis();
        Metric m = new Metric(metricName, ts,
                idPrefix + "Executor",
                idPrefix + "Comp",
                idPrefix + "Stream" + random.nextInt(),
                idPrefix + "Topo",
                123.45);
        m.setHost("testHost");
        return m;
    }

    private Metric makeAggMetric(String metricName) {
        Metric m = makeMetric(metricName);
        m.setAggLevel((byte) 1);
        m.setValue(100.0);
        for (int i = 1; i < 10; ++i) {
            m.updateAverage(100.00 * i);
        }
        return m;
    }


    public void run() {
        LOG.info("Running for node with id = {}", idPrefix);

        for (int i = 0; i < rounds; ++i) {
            writeTimer.put(i, new ArrayList<Integer>(writeCount));
            aggWriteTimer.put(i, new ArrayList<Integer>(writeCount));
            scanTimer.put(i, new ArrayList<Integer>(scanCount));
            insert(i);
            scan(i);
            LOG.info("Round {} complete for {}", i, idPrefix);
        }

        LOG.info("Benchmark complete for {}", idPrefix);
        LOG.info("Write timer {} = {}", idPrefix, writeTimer);
        LOG.info("Scan timer {} = {}", idPrefix, scanTimer);
    }


    private void insert(int round) {
        for (int i = 0; i <= writeCount; ++i) {
            long startTime = timeNow();
            for (String metricName : metricNames) {
                Metric m = makeMetric(metricName);
                store.insert(m);
            }
            long endTime = timeNow();
            int duration = (int) (endTime - startTime);
            writeTimer.get(round).add(duration);
        }


        for (int i = 0; i <= writeCount; ++i) {
            long startTime = timeNow();
            for (String metricName : metricNames) {
                Metric m = makeAggMetric(metricName);
                store.insert(m);
            }
            long endTime = timeNow();
            int duration = (int) (endTime - startTime);
            aggWriteTimer.get(round).add(duration);
        }
    }

    private void scan(int round) {

        HashMap<String, Object> settings = new HashMap<>();
        HashSet<Metric> metricsRetrieved = new HashSet<>();
        for (int i = 0; i <= scanCount; ++i) {

            long startTime = timeNow();
            store.scan(settings, (metric, timeRange) -> {
                metricsRetrieved.add(metric);
            });
            long endTime = timeNow();
            int duration = (int) (endTime - startTime);
            scanTimer.get(round).add(duration);

            LOG.info("Scanned {} metrics", metricsRetrieved.size());
            metricsRetrieved.clear();
        }
    }


}
