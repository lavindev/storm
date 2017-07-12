package org.apache.storm.metrics2.store;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.storm.generated.Window;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.*;


@RunWith(Parameterized.class)
public class AggregatingMetricStoreTest {
    private final static Logger LOG = LoggerFactory.getLogger(AggregatingMetricStoreTest.class);

    private static HBaseTestingUtility hbaseTestingUtility;
    private static HBaseStore hbaseStore;
    private static RocksDBStore rocksDBStore;
    private static MetricStore underlyingStore;
    private AggregatingMetricStore store;
    private Random random;

    // CONFIG
    private static final String ROCKS_DB_LOCATION = "/tmp/storm_rocks";

    public enum StoreType {
        ROCKSDB, HBASE
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {StoreType.ROCKSDB}, {StoreType.HBASE}
        });
    }

    private static void initHBaseSchema(Map conf, HBaseAdmin admin) {
        try {
            HBaseSchema                                      schema   = new HBaseSchema(conf);
            HashMap<TableName, ArrayList<HColumnDescriptor>> tableMap = schema.getTableMap();

            for (Map.Entry<TableName, ArrayList<HColumnDescriptor>> entry : tableMap.entrySet()) {

                TableName               name        = entry.getKey();
                List<HColumnDescriptor> columnsList = entry.getValue();

                HTableDescriptor descriptor = new HTableDescriptor(name);
                for (HColumnDescriptor columnDescriptor : columnsList) {
                    descriptor.addFamily(columnDescriptor);
                }
                admin.createTable(descriptor);

            }
        } catch (Exception e) {
            Assert.fail("Unexpected exception - " + e);
        }
    }

    @BeforeClass
    public static void setUp() {

        try {
            // set up hbase
            HashMap<String, Object> hbaseConf = makeConfig(StoreType.HBASE);
            hbaseTestingUtility = new HBaseTestingUtility();
            hbaseTestingUtility.startMiniCluster();
            initHBaseSchema(hbaseConf, hbaseTestingUtility.getHBaseAdmin());
            // set ZK info from test cluster
            int zkPort = hbaseTestingUtility.getZkCluster().getClientPort();
            hbaseConf.put("HBaseZookeeperPortOverride", zkPort);
            hbaseStore = new HBaseStore();
            hbaseStore.prepare(hbaseConf);

            // set up rocks db
            HashMap<String, Object> rocksConf = makeConfig(StoreType.ROCKSDB);
            rocksDBStore = new RocksDBStore();
            rocksDBStore.prepare(rocksConf);

        } catch (Exception e) {
            LOG.error("Unhandled exception - ", e);
            fail();
        }
    }

    @AfterClass
    public static void tearDown() {
        // shutdown HBase utility
        try {
            hbaseTestingUtility.shutdownMiniCluster();
        } catch (Exception e) {
            LOG.error("Could not tear down HBase cluster - ", e);
            fail();
        }

        // delete tmp RocksDB
        try {
            File tmpRocksDB = FileUtils.getTempDirectory();
            FileUtils.deleteDirectory(tmpRocksDB);
        } catch (IOException e) {
            LOG.error("Could not delete tmp RocksDB directory - ", e);
        }
    }

    public AggregatingMetricStoreTest(StoreType type) {

        random = new Random(System.currentTimeMillis());

        switch (type) {
            case ROCKSDB:
                underlyingStore = rocksDBStore;
                break;
            case HBASE:
                underlyingStore = hbaseStore;
                break;
        }

        store = new AggregatingMetricStore(underlyingStore);

    }

    private static HashMap<String, Object> makeConfig(StoreType type) {

        HashMap<String, Object> conf = new HashMap<>();

        switch (type) {
            case ROCKSDB:
                String tmpRocksDB = FileUtils.getTempDirectoryPath();
                conf.put("storm.metrics2.store.rocksdb.create_if_missing", true);
                conf.put("storm.metrics2.store.rocksdb.location", tmpRocksDB);
                conf.put("storm.metrics2.store.rocksdb.retention", 1);
                conf.put("storm.metrics2.store.rocksdb.retention.units", "MINUTES");
                break;
            case HBASE:
                // metadata map
                HashMap<String, Object> metaDataMap = new HashMap<>();
                List<String> metadataNames = Arrays.asList("topoMap", "streamMap", "hostMap",
                        "compMap", "metricMap", "executorMap");

                metadataNames.forEach((name) -> {
                    HashMap<String, String> m = new HashMap<>();
                    m.put("name", "metrics");
                    m.put("cf", "m");
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
                metricsMap.put("columns", columnsMap);

                // schema map
                HashMap<String, Object> schemaMap = new HashMap<>();
                schemaMap.put("type", "expanded");
                schemaMap.put("metrics", metricsMap);
                schemaMap.put("metadata", metaDataMap);

                conf.put("storm.metrics2.store.HBaseStore.hbase.schema", schemaMap);
                break;
        }

        return conf;
    }


    private static Metric makeMetric(long tstamp) {
        Metric m = new Metric("metric" + tstamp,
                tstamp,
                "exec" + tstamp,
                "comp" + tstamp,
                "stream" + tstamp,
                "topo",
                456.2);
        m.setAggLevel((byte) 0);
        m.setHost("testHost");
        return m;
    }

    private void assertMetricExists(Metric m) {

        Metric newMetric = new Metric(m);
        newMetric.setValue(0.00);
        newMetric.setCount(0L);

        underlyingStore.populateValue(newMetric);

        assertEquals(m, newMetric);

    }

    private void assertAggregationsExist(Metric m) {

        Metric     aggMetric = new Metric(m);
        List<Long> buckets   = store.getBuckets();

        for (Long bucket : buckets) {
            aggMetric.setValue(0.00);
            aggMetric.setCount(0L);

            long msToBucket      = 1000L * 60L * bucket;
            Long roundedToBucket = msToBucket * (m.getTimeStamp() / msToBucket);

            aggMetric.setAggLevel(bucket.byteValue());
            aggMetric.setTimeStamp(roundedToBucket);

            underlyingStore.populateValue(aggMetric);

            assertNotEquals(aggMetric.getValue(), 0.00);
            assertNotEquals(aggMetric.getCount(), 0L);
        }
    }


    @Test
    public void testInsert() {
        Metric m = makeMetric(999);
        store.insert(new Metric(m));
        assertMetricExists(new Metric(m));
        assertAggregationsExist(new Metric(m));
    }

    @Test
    public void testAggregateUpdate() {

        final double value1 = 2000.00, value2 = 4000.00;

        // insert 2 metrics with timestamps within the same minute
        final long ts = (60 * 60 * 1000) + (25 * 60 * 1000) + (20 * 1000); // 1 hour, 25 minutes, 20 seconds
        Metric     m1 = makeMetric(ts);
        m1.setValue(value1);

        Metric m2 = new Metric(m1);
        m2.setTimeStamp(ts + (20 * 1000));
        m2.setValue(value2);

        store.insert(m1);
        assertMetricExists(m1);
        assertAggregationsExist(m1);

        store.insert(m2);
        assertMetricExists(m2);
        assertAggregationsExist(m2);

        Metric     aggMetric = new Metric(m1);
        List<Long> buckets   = store.getBuckets();

        for (Long bucket : buckets) {
            aggMetric.setCount(0L);
            aggMetric.setValue(0.00);

            long msToBucket      = 1000L * 60L * bucket;
            Long roundedToBucket = msToBucket * (ts / msToBucket);

            aggMetric.setAggLevel(bucket.byteValue());
            aggMetric.setTimeStamp(roundedToBucket);

            underlyingStore.populateValue(aggMetric);

            assertEquals(2L, aggMetric.getCount());
            assertEquals((value1 + value2), aggMetric.getValue(), 0.0001);
            assertEquals(value1 + value2, aggMetric.getSum(), 0.0001);
            assertEquals(Math.min(value1, value2), aggMetric.getMin(), 0.0001);
            assertEquals(Math.max(value1, value2), aggMetric.getMax(), 0.0001);
        }
    }

    @Test
    public void testPopulateValue() {

        Metric m = makeMetric(9999);
        underlyingStore.insert(m);

        m.setValue(0.00);
        m.setCount(0L);

        store.populateValue(m);

        assertNotEquals(m.getValue(), 0.00);
        assertNotEquals(m.getCount(), 0L);

    }

    @Test
    public void testScan() {

        final int    COUNT  = 25;
        final String topoId = "testScanTopology";

        ArrayList<Metric> metricsInserted  = new ArrayList<>(COUNT);
        ArrayList<Metric> metricsRetrieved = new ArrayList<>(COUNT);

        HashMap<String, Object> settings = new HashMap<>();
        settings.put(StringKeywords.aggLevel, 0);
        settings.put(StringKeywords.topoId, topoId);

        TimeRange          timeRangeAll = new TimeRange(100L, 100L + COUNT, Window.ALL);
        HashSet<TimeRange> timeRangeSet = new HashSet<>();
        timeRangeSet.add(timeRangeAll);
        settings.put(StringKeywords.timeRangeSet, timeRangeSet);
        // insert metrics
        for (int i = 0; i < COUNT; ++i) {
            Metric m = makeMetric(100 + i);
            m.setTopoIdStr(topoId);
            underlyingStore.insert(m);
            metricsInserted.add(m);
        }


        // scan for inserted metrics, check that we have all inserted
        store.scan(settings, (metric, timeRange) -> metricsRetrieved.add(metric));
        boolean containsAll = metricsRetrieved.containsAll(metricsInserted);
        assertTrue(containsAll);
    }

    @Test
    public void testRemove() {

        final int    COUNT  = 25;
        final String topoId = "testRemoveTopology";

        ArrayList<Metric> metricsInserted  = new ArrayList<>(COUNT);
        ArrayList<Metric> metricsRetrieved = new ArrayList<>(COUNT);

        HashMap<String, Object> settings = new HashMap<>();
        settings.put(StringKeywords.aggLevel, 0);
        settings.put(StringKeywords.topoId, topoId);

        TimeRange          timeRangeAll = new TimeRange(200L, 200L + COUNT, Window.ALL);
        HashSet<TimeRange> timeRangeSet = new HashSet<>();
        timeRangeSet.add(timeRangeAll);
        settings.put(StringKeywords.timeRangeSet, timeRangeSet);

        // insert metrics
        for (int i = 0; i < COUNT; ++i) {
            Metric m = makeMetric(200 + i);
            m.setTopoIdStr(topoId);
            underlyingStore.insert(m);
            metricsInserted.add(m);
        }

        // scan for inserted metrics, check that we have all inserted
        underlyingStore.scan(settings, (metric, timerange) -> {
            metricsRetrieved.add(metric);
        });

        boolean containsAll = metricsRetrieved.containsAll(metricsInserted);
        assertTrue(containsAll);
        metricsRetrieved.clear();

        // remove inserted metrics
        store.remove(settings);

        // scan again, assert we have nil
        underlyingStore.scan(settings, (metric, timerange) -> {
            metricsRetrieved.add(metric);
        });

        assertEquals(0, metricsRetrieved.size());

    }

}
