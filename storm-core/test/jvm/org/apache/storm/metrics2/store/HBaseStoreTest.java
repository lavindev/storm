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


import clojure.lang.Obj;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HConstants;
import org.apache.storm.utils.Time;

public class HBaseStoreTest {

    private final static String HBASE_ROOT_DIR = "/tmp/hbase";
    private final static String HBASE_METRICS_TABLE = "metrics";
    private final static int RETENTION = 1;
    private final static String RETENTION_UNITS = "MINUTES";
    private final static String ZOOKEEPER_ROOT = "/storm";
    private final static List<String> ZOOKEEPER_SERVERS = Arrays.asList("localhost");
    private final static int ZOOKEEPER_PORT = 2181;


    private Map makeConfig() {

        HashMap<String, Object> confMap = new HashMap<String, Object>();

        confMap.put("storm.metrics2.store.HBaseStore.hbase.root_dir", HBASE_ROOT_DIR);
        confMap.put("storm.metrics2.store.HBaseStore.hbase.metrics_table", HBASE_METRICS_TABLE);
        confMap.put("storm.metrics2.store.HBaseStore.retention", RETENTION);
        confMap.put("storm.metrics2.store.HBaseStore.retention.units", RETENTION_UNITS);
        confMap.put("storm.zookeeper.servers", ZOOKEEPER_SERVERS);
        confMap.put("storm.zookeeper.port", ZOOKEEPER_PORT);
        confMap.put("storm.zookeeper.root", ZOOKEEPER_ROOT);

        // metadata map
        HashMap<String, Object> metaDataMap = new HashMap<String, Object>();
        List<String> metadataNames = Arrays.asList("topoMap", "streamMap", "hostMap",
                "compMap", "metricMap", "executorMap");

        metadataNames.forEach((name) -> {
            HashMap<String, String> m = new HashMap<String, String>();
            m.put("name", name);
            m.put("cf", "c");
            m.put("column", "c");
            metaDataMap.put(name, m);
        });

        // columns map & metrics map
        HashMap<String, String> columnsMap = new HashMap<String, String>();
        columnsMap.put("value", "v");
        columnsMap.put("sum", "s");
        columnsMap.put("count", "c");
        columnsMap.put("min", "i");
        columnsMap.put("max", "a");

        HashMap<String, Object> metricsMap = new HashMap<String, Object>();
        metricsMap.put("name", "metrics");
        metricsMap.put("cf", "c");
        metricsMap.put("columns", columnsMap);

        // schema map
        HashMap<String, Object> schemaMap = new HashMap<String, Object>();
        schemaMap.put("metrics", metricsMap);
        schemaMap.put("metadata", metaDataMap);

        confMap.put("storm.metrics2.store.HBaseStore.hbase.schema", schemaMap);


        return confMap;
    }

    private Metric makeMetric() {
        long ts = (long) Time.currentTimeSecs();
        return makeMetric(ts);
    }

    private Metric makeMetric(long ts) {

        Metric m = new Metric("testMetric" + ts, ts,
                "testExecutor" + ts,
                "testComp" + ts,
                "testStream" + ts,
                "testTopo" + ts,
                123.45);
        m.setHost("testHost" + ts);
        return m;
    }

    private Metric makeAggMetric() {
        long ts = (long) Time.currentTimeSecs();
        return makeAggMetric(ts);
    }

    private Metric makeAggMetric(long ts) {

        Metric m = makeMetric(ts);
        m.setAggLevel((byte) 1);
        m.setValue(100.0);

        for (int i = 1; i < 10; ++i) {
            m.updateAverage(100.00 * i);
        }

        return m;
    }

    @Test
    public void testPrepare() {
        HBaseStore store = new HBaseStore();
        Map conf = makeConfig();
        try {
            store.prepare(conf);
        } catch (Exception e) {
            fail("Unexpected exception" + e);
        }
    }

    @Test
    public void testPrepareInvalidConf() {
        HBaseStore store = new HBaseStore();
        Map conf = makeConfig();

        // call prepare() with one config entry missing each iteration
        for (Object key : conf.keySet()) {
            TreeMap<String, Object> testMap = new TreeMap<String, Object>(conf);
            testMap.remove((String) key);

            boolean exceptionThrown = false;
            try {
                store.prepare(testMap);
            } catch (MetricException e) {
                exceptionThrown = true;
            }
            assertTrue(exceptionThrown);
        }

    }

    @Test
    public void testInsert() {

        Logger log = Mockito.mock(Logger.class);

        HBaseStore store = new HBaseStore();
        Map conf = makeConfig();
        Metric m = makeMetric();

        try {
            store.prepare(conf);
        } catch (MetricException e) {
            fail("Unexpected Exception" + e);
        }

        store.insert(m);
        Mockito.verifyZeroInteractions(log);
    }

    @Test
    public void testInsertAgg() {

        Logger log = Mockito.mock(Logger.class);

        HBaseStore store = new HBaseStore();
        Map conf = makeConfig();
        Metric m = makeAggMetric();

        try {
            store.prepare(conf);
        } catch (MetricException e) {
            fail("Unexpected Exception" + e);
        }

        store.insert(m);
        Mockito.verifyZeroInteractions(log);
    }


}