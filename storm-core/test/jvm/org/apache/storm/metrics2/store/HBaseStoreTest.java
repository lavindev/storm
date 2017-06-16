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


import java.util.*;

import org.apache.hadoop.hbase.*;
import org.apache.storm.generated.Window;
import org.junit.*;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.storm.utils.Time;
import org.apache.hadoop.hbase.HBaseTestingUtility;

public class HBaseStoreTest {

    // manual parameters
    private final static String DEFAULT_ZK_PREFIX = "storm.metrics2.store.HBaseStore.zookeeper";
    private final static String SCHEMA_TYPE = "compact";

    private final static String HBASE_ROOT_DIR = "/tmp/hbase";
    private final static String ZOOKEEPER_ROOT = "/storm";
    private final static List<String> ZOOKEEPER_SERVERS = Arrays.asList("localhost");
    private final static int ZOOKEEPER_PORT = 2181;
    private final static int ZOOKEEPER_SESSION_TIMEOUT = 20000;

    private static HBaseStore store;
    private static HBaseTestingUtility testUtil;
    private static Table metricsTable;
    private static Random random = new Random();

    private static HashMap<String, Object> makeConfig() {
        return makeConfig(DEFAULT_ZK_PREFIX);
    }

    private static HashMap<String, Object> makeConfig(String zkPrefix) {

        HashMap<String, Object> confMap = new HashMap<>();

        confMap.put("storm.metrics2.store.HBaseStore.hbase.root_dir", HBASE_ROOT_DIR);
        confMap.put(zkPrefix + ".servers", ZOOKEEPER_SERVERS);
        confMap.put(zkPrefix + ".port", ZOOKEEPER_PORT);
        confMap.put(zkPrefix + ".root", ZOOKEEPER_ROOT);
        confMap.put(zkPrefix + ".session.timeout", ZOOKEEPER_SESSION_TIMEOUT);

        // metadata map
        HashMap<String, Object> metaDataMap = new HashMap<>();
        List<String> metadataNames = Arrays.asList("topoMap", "streamMap", "hostMap",
                "compMap", "metricMap", "executorMap");

        metadataNames.forEach((name) -> {
            HashMap<String, String> m = new HashMap<>();
            m.put("name", name);
            m.put("cf", "c");
            m.put("column", "c");
            m.put("refcounter", "REFCOUNTER");
            metaDataMap.put(name, m);
        });

        // columns map & metrics map
        HashMap<String, String> columnsMap = new HashMap<>();
        columnsMap.put("value", "v");
        columnsMap.put("sum", "s");
        columnsMap.put("count", "c");
        columnsMap.put("min", "i");
        columnsMap.put("max", "a");

        HashMap<String, Object> metricsMap = new HashMap<>();
        metricsMap.put("name", "metrics");
        metricsMap.put("cf", "c");
        if (SCHEMA_TYPE.equals("compact"))
            metricsMap.put("column", "c");
        else if (SCHEMA_TYPE.equals("expanded"))
            metricsMap.put("columns", columnsMap);

        // schema map
        HashMap<String, Object> schemaMap = new HashMap<>();
        schemaMap.put("type", SCHEMA_TYPE);
        schemaMap.put("metrics", metricsMap);
        schemaMap.put("metadata", metaDataMap);

        confMap.put("storm.metrics2.store.HBaseStore.hbase.schema", schemaMap);


        return confMap;
    }

    private static Metric makeMetric(long ts) {

        Metric m = new Metric("testMetric", ts,
                "testExecutor",
                "testComp",
                "testStream" + ts,
                "testTopo",
                123.45);
        m.setHost("testHost");
        m.setValue(123.45);
        return m;
    }

    private static Metric makeAggMetric(long ts) {

        Metric m = makeMetric(ts);
        m.setAggLevel((byte) 1);
        m.setValue(100.0);

        for (int i = 1; i < 10; ++i) {
            m.updateAverage(100.00 * i);
        }

        return m;
    }

    private static Configuration createHBaseConfiguration(Map config) {

        Configuration conf = HBaseConfiguration.create();

        Object zookeeperServers = config.get("storm.metrics2.store.HBaseStore.zookeeper.servers");
        String zkPrefix = (zookeeperServers == null) ? "storm.zookeeper" : "storm.metrics2.store.HBaseStore.zookeeper";

        String hbaseRootDir = (String) config.get("storm.metrics2.store.HBaseStore.hbase.root_dir");
        String zookeeperQuorum = String.join(";", (List) config.get(zkPrefix + ".servers"));
        String zookeeperRoot = (String) config.get(zkPrefix + ".root");
        int zookeeperPort = (int) config.get(zkPrefix + ".port");
        int zookeeperSessionTimeout = (int) config.get(zkPrefix + ".session.timeout");

        conf.set(HConstants.HBASE_DIR, hbaseRootDir);
        conf.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum);
        conf.set(HConstants.ZOOKEEPER_DATA_DIR, zookeeperRoot);
        conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zookeeperPort);
        conf.setInt(HConstants.ZK_SESSION_TIMEOUT, zookeeperSessionTimeout);

        // security
        //conf.set("hbase.security.authentication", "kerberos");
        //conf.set("hbase.rpc.protection", "privacy");

        return conf;
    }


    @BeforeClass
    public static void setUp() {

        HashMap<String, Object> conf = makeConfig();
        store = new HBaseStore();
        Configuration hbaseConf = createHBaseConfiguration(conf);

        testUtil = new HBaseTestingUtility(hbaseConf);

        try {
            testUtil.startMiniCluster();

            // set ZK info from test cluster - not the same as passed in above
            int zkPort = testUtil.getZkCluster().getClientPort();
            conf.put("storm.metrics2.store.HBaseStore.zookeeper.port", zkPort);
            conf.put("storm.zookeeper.port", zkPort);

            store.prepare(conf);
            metricsTable = store.getMetricsTable();
        } catch (Exception e) {
            fail("Unexpected exception" + e);
        }

    }

    @AfterClass
    public static void tearDown() {
        try {
            testUtil.shutdownMiniCluster();
        } catch (Exception e) {
            fail("Unexpected - " + e);
        }
    }

    @Test
    public void testPrepareInvalidConf() {
//        HBaseStore store = new HBaseStore();
//        // call prepare() with one config entry missing each iteration
//        for (Object key : conf.keySet()) {
//            TreeMap<String, Object> testMap = new TreeMap<String, Object>(conf);
//            testMap.remove((String) key);
//
//            boolean exceptionThrown = false;
//            try {
//                store.prepare(testMap);
//            } catch (MetricException e) {
//                exceptionThrown = true;
//            }
//            assertTrue(exceptionThrown);
//        }
    }

    @Test
    public void testInsert() {
        Metric m = makeMetric(1234567);
        store.insert(m);

        ResultScanner scanner;
        Scan s = new Scan();
        try {
            scanner = metricsTable.getScanner(s);
        } catch (Exception e) {
            fail("Unexpected - " + e);
            return;
        }

        for (Result result : scanner) {
            long ts = result.rawCells()[0].getTimestamp();
            if (m.getTimeStamp() == ts) {
                return;
            }
        }

        fail("Could not find metric in store.");

    }

    @Test
    public void testInsertAgg() {
        Metric m = makeAggMetric(1234567);
        store.insert(m);

        ResultScanner scanner;
        Scan s = new Scan();
        try {
            scanner = metricsTable.getScanner(s);
        } catch (Exception e) {
            fail("Unexpected - " + e);
            return;
        }

        for (Result result : scanner) {
            long ts = result.rawCells()[0].getTimestamp();
            if (m.getTimeStamp() == ts) {
                return;
            }
        }

        fail("Could not find metric in store.");
    }

    @Test
    public void testScan() {

        ArrayList<Metric> metricsList = new ArrayList<>(10);

        for (int i = 1; i <= 10; i++) {
            Metric m = makeMetric((long) random.nextInt(999));
            metricsList.add(m);
            store.insert(m);
        }

        Integer aggLevel = metricsList.get(0).getAggLevel().intValue();
        String topoIdStr = metricsList.get(0).getTopoIdStr();

        HashMap<String, Object> settings = new HashMap<>();
        settings.put(StringKeywords.aggLevel, aggLevel);
        settings.put(StringKeywords.topoId, topoIdStr);

        store.scan(settings, (metric, timeRanges) -> assertTrue(metricsList.contains(metric)));

    }

    @Test
    public void testPopulateValue() {

        Metric m = makeMetric(23894);
        store.insert(m);

        Metric newMetric = makeMetric(23894);
        newMetric.setValue(0.00);
        newMetric.setCount(0L);

        // check that newMetric has values populated from inserted metric
        store.populateValue(newMetric);
        assertNotEquals(0L, newMetric.getCount());
        assertNotEquals(0.00, newMetric.getValue(), 0.00001);
        assertNotEquals(0.00, newMetric.getSum(), 0.00001);
        assertNotEquals(0.00, newMetric.getMin(), 0.00001);
        assertNotEquals(0.00, newMetric.getMax(), 0.00001);

        // check that invalid new metric isn't populated
        newMetric.setTopoIdStr("BAD TOPOLOGY");
        newMetric.setValue(0.00);
        newMetric.setCount(0L);
        store.populateValue(newMetric);
        assertEquals(0L, newMetric.getCount());
        assertEquals(0.00, newMetric.getValue(), 0.00001);
        assertEquals(0.00, newMetric.getSum(), 0.00001);
        assertEquals(0.00, newMetric.getMin(), 0.00001);
        assertEquals(0.00, newMetric.getMax(), 0.00001);

    }

    @Test
    public void testRemove() {

        for (int i = 1; i <= 10; ++i) {
            Metric m = makeMetric(i);
            store.insert(m);
        }
        for (int i = 101; i <= 110; ++i) {
            Metric m = makeMetric(i);
            store.insert(m);
        }

        HashMap<String, Object> settings = new HashMap<>();
        HashSet<TimeRange> timeRangeSet = new HashSet<>();
        timeRangeSet.add(new TimeRange(1L, 10L + 1L, Window.ALL));
        timeRangeSet.add(new TimeRange(101L, 110L + 1L, Window.ALL));

        settings.put(StringKeywords.aggLevel, 0);
        settings.put(StringKeywords.topoId, "testTopo");
        settings.put(StringKeywords.timeRangeSet, timeRangeSet);

        // scan for inserted metrics, should have all 20
        HashSet<Metric> retrievedMetrics = new HashSet<>();

        store.scan(settings, (metric, timeRanges) -> retrievedMetrics.add(metric));

        assertEquals(20, retrievedMetrics.size());

        retrievedMetrics.clear();

        // remove metrics
        store.remove(settings);

        // scan again, should have nil
        store.scan(settings, (metric, timeRanges) -> retrievedMetrics.add(metric));

        assertEquals(0, retrievedMetrics.size());

    }


}