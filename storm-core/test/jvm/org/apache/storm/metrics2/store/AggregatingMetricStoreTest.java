package org.apache.storm.metrics2.store;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AggregatingMetricStoreTest {

    @Test
    public void testInserting() {
        MetricStore storeMock = mock(MetricStore.class);
        AggregatingMetricStore s = new AggregatingMetricStore(storeMock);
        Metric m = new Metric("emitted", 
                              1483228800001L,
                              "[1-1]",
                              "word",
                              "default",
                              "topo-1",
                              1.0);
        s.insert(m);
        assertEquals(m.getTimeStamp(), 1483228800000L);
    }
}
