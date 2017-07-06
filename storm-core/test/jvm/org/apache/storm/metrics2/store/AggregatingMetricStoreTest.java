package org.apache.storm.metrics2.store;

import org.apache.commons.codec.binary.Hex;
import org.apache.storm.generated.Window;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

//TODO: this should be an internal enum

public class AggregatingMetricStoreTest {

    @Test
    public void testInserting() {
        MetricStore            storeMock = mock(MetricStore.class);
        AggregatingMetricStore s         = new AggregatingMetricStore(storeMock);
        Metric m = new Metric("emitted",
                1483228800001L,
                "[1-1]",
                "word",
                "default",
                "topo-1",
                1.0);
        s.insert(m);
        assertEquals((long) m.getTimeStamp(), 1483228800000L);
    }

    public Metric makeMetric(long tstamp) {
        Metric m = new Metric("metric" + tstamp, tstamp, "exec" + tstamp, "comp" + tstamp, "stream" + tstamp,
                "topo", 456.2);
        m.setAggLevel((byte) 0);
        m.setHost("testHost");
        return m;
    }

    @Test
    public void prefixCreationTest() {
        Aggregation agg = new Aggregation();
        agg.filterTime(1L, 100L, Window.ALL);
        Map               settings   = agg.getSettings();
        RocksDBSerializer serializer = new RocksDBSerializer();
        byte[]            prefix     = serializer.createPrefix(settings);
        System.out.println("prefix is " + Hex.encodeHexString(prefix));
    }

    @Test
    public void testInsertRetrieval() {
        RocksDBStore            c      = new RocksDBStore();
        HashMap<String, String> config = new HashMap<String, String>();
        config.put("storm.metrics2.store.rocksdb.location", "/tmp/testing_rocksdb");
        config.put("storm.metrics2.store.rocksdb.create_if_missing", "true");
        c.prepare(config);

        c.insert(makeMetric(1L));
        c.insert(makeMetric(2L));
        c.insert(makeMetric(3L));
        c.insert(makeMetric(4L));

        System.out.println("full scan");
        c.scan((metric, timeRanges) -> {
            System.out.println(metric.toString());
        });


        try {
            for (int i = 0; i < 10; i++) {
                Aggregation agg = new Aggregation();
                agg.filterTime((long) i, (long) i + 1, Window.ALL);
                System.out.println("scan " + i + " to " + (i + 1));
                c.scan(agg.getSettings(), (metric, timeRanges) -> {
                    System.out.println("here" + metric.toString());
                });
                Thread.sleep(1000);
            }
        } catch (InterruptedException ie) {
        }

        Aggregation agg = new Aggregation();
        System.out.println("scan 2 to 3");
        agg.filterTime(2L, 3L, Window.ALL);
        c.scan(agg.getSettings(), (metric, timeRanges) -> {
            System.out.println(metric.toString());
        });
    }

//    @Test
//    public void bucketizationTest() {
//        MetricStore            storeMock = mock(MetricStore.class);
//        AggregatingMetricStore s         = new AggregatingMetricStore(storeMock);
//        TimeRange              t         = new TimeRange((5 * 3600L * 1000) - 5000, (20 * 3600L * 1000) + 5000, Window.ALL);
//        TimeRange              tall      = new TimeRange(0L, null, Window.ALL);
//        s.bucketize(t, 60 * 60 * 1000);
//        s.bucketize(t, 60 * 10 * 1000);
//        s.bucketize(tall, 60 * 60 * 1000);
//    }
}
