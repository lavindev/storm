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


import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
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
    private final static String SCHEMA = "compact";
    private final static String HBASE_METRICS_TABLE = "testMetrics";
    private final static int RETENTION = 1;
    private final static String RETENTION_UNITS = "MINUTES";
    private final static String ZOOKEEPER_ROOT = "/storm";
    private final static List<String> ZOOKEEPER_SERVERS = Arrays.asList("localhost");
    private final static int ZOOKEEPER_PORT = 2181;

    private HBaseStore store;
    private Map conf;

    private Map makeConfig() {

        HashMap<String, Object> confMap = new HashMap<String, Object>();

        confMap.put("storm.metrics2.store.HBaseStore.hbase.root_dir", HBASE_ROOT_DIR);
        confMap.put("storm.metrics2.store.HBaseStore.hbase.metrics_table", HBASE_METRICS_TABLE);
        confMap.put("storm.metrics2.store.HBaseStore.schema", SCHEMA);
        confMap.put("storm.metrics2.store.HBaseStore.retention", RETENTION);
        confMap.put("storm.metrics2.store.HBaseStore.retention.units", RETENTION_UNITS);
        confMap.put("storm.zookeeper.servers", ZOOKEEPER_SERVERS);
        confMap.put("storm.zookeeper.port", ZOOKEEPER_PORT);
        confMap.put("storm.zookeeper.root", ZOOKEEPER_ROOT);

        return confMap;
    }

    private Metric makeMetric() {
        long ts = new Date().getTime();
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
        long ts = new Date().getTime();
        return makeAggMetric(ts);
    }

    private Metric makeAggMetric(long ts) {

        Metric m = makeMetric(ts);
        m.setAggLevel((byte) 60);
        m.setValue(100.0);

        for (int i = 1; i < 10; ++i) {
            m.updateAverage(100.00 * i);
        }

        return m;
    }

    @Before
    public void setUp() {
        this.store = new HBaseStore();
        this.conf = makeConfig();
    }

    @After
    public void tearDown() {
        store.close();
    }

    @Test
    public void testPrepare() {
        try {
            store.prepare(conf);
        } catch (Exception e) {
            fail("Unexpected exception " + e);
        }
    }

    @Test
    public void testPrepareInvalidConf() {

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
    public void testPutGet() {

        final byte[] key = Bytes.toBytes("testKey");
        final byte[] value = Bytes.toBytes("testValue");
        byte[] lookup = new byte[value.length];

        try {
            store.prepare(conf);
        } catch (MetricException e) {
            fail("Unexpected Exception" + e);
        }

        try {
            store.put(key, value);
            lookup = store.get(key);
        } catch (IOException e) {
            fail("Unexpected Exception " + e);
        }

        assertEquals(Bytes.toString(value), Bytes.toString(lookup));
    }

    @Test
    public void testInsert() {

        Logger log = Mockito.mock(Logger.class);
        Metric m = makeMetric(12345);

        try {
            store.prepare(conf);
        } catch (MetricException e) {
            fail("Unexpected Exception" + e);
        }

        store.insert(m);

        // Assertions/Verifications

        Mockito.verifyZeroInteractions(log);

        // TODO: do an actual lookup? need key
    }

    @Test
    public void testInsertAgg() {

        Logger log = Mockito.mock(Logger.class);
        Metric m = makeAggMetric();

        try {
            store.prepare(conf);
        } catch (MetricException e) {
            fail("Unexpected Exception" + e);
        }

        store.insert(m);
        Mockito.verifyZeroInteractions(log);

    }

    @Test
    public void testPopulateValue() {

        try {
            store.prepare(conf);
        } catch (MetricException e) {
            fail("Unexpected Exception" + e);
        }

        Metric m = makeMetric(999);
        store.insert(m);
        m.setValue(0.00);
        m.setCount(0);
        store.populateValue(m);
        assertNotEquals(m.getValue(), 0.00, 0.0001);
        assertNotEquals(m.getCount(), 0);
    }

    @Test
    public void testPopulateValueAgg() {

        try {
            store.prepare(conf);
        } catch (MetricException e) {
            fail("Unexpected Exception" + e);
        }

        Metric m = makeAggMetric(1000);
        store.insert(m);
        m.setValue(0.00);
        m.setCount(0);

        store.populateValue(m);
        assertNotEquals(m.getCount(), 0);
        assertNotEquals(m.getValue(), 0.00, 0.0001);
        assertNotEquals(m.getSum(), 0.00, 0.0001);
        assertNotEquals(m.getMin(), 0.00, 0.0001);
        assertNotEquals(m.getMax(), 0.00, 0.0001);

    }

    @Test
    public void testScanSimple() {

        try {
            store.prepare(conf);
        } catch (MetricException e) {
            fail("Unexpected Exception" + e);
        }

        store.insert(makeMetric(1));
        store.insert(makeMetric(2));
        store.insert(makeMetric(3));
        store.insert(makeMetric(4));

        store.scan((metric, timeRanges) -> System.out.println(metric.toString()));

    }

}