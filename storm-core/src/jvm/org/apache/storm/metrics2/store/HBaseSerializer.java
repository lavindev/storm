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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import static org.apache.storm.metrics2.store.HBaseSerializer.MetaDataIndex.*;

public class HBaseSerializer {
    private final static Logger LOG = LoggerFactory.getLogger(HBaseSerializer.class);

    private class MetaData {
        public HashMap<String, Integer> map;
        public HashMap<Integer, String> rmap;
        public Table table;
    }

    public enum MetaDataIndex {
        TOPOLOGY(0),
        STREAM(1),
        HOST(2),
        COMP(3),
        METRICNAME(4),
        EXECUTOR(5);

        private int _index;

        MetaDataIndex(int index) {
            this._index = index;
        }

        public int index() {
            return _index;
        }

        public static int count() {
            return MetaDataIndex.values().length;
        }
    }

    private final static byte[] REFCOUNTER = Bytes.toBytes("REFCOUNTER");

    private Connection _hbaseConnection;
    private AggregationClient _aggregationClient;
    private HBaseSchema _schema;

    private Table metricsTable;
    private MetaData[] metaData;

    public HBaseSerializer(Connection hbaseConnection, HBaseSchema schema) {

        // TODO: fix configuration lookup
        this._hbaseConnection = hbaseConnection;
        Configuration conf = hbaseConnection.getConfiguration();
        this._aggregationClient = new AggregationClient(conf);
        this._schema = schema;
        this.metaData = new MetaData[MetaDataIndex.count()];

        assignMetricsTable();

        for (MetaDataIndex index : MetaDataIndex.values()) {
            assignMetaDataTable(index);
            initializeMap(index);
        }

    }

    private void assignMetricsTable() {

        try {
            Admin hbaseAdmin = _hbaseConnection.getAdmin();
            TableName name = _schema.metricsTableInfo.getTableName();
            HTableDescriptor descriptor = _schema.metricsTableInfo.getDescriptor();

            if (!hbaseAdmin.tableExists(name)) {
                hbaseAdmin.createTable(descriptor);
            }

            this.metricsTable = _hbaseConnection.getTable(_schema.metricsTableInfo.getTableName());

        } catch (IOException e) {
            LOG.error("Could not assign metrics table", e);
        }

    }

    private void assignMetaDataTable(MetaDataIndex index) {
        int i = index.ordinal();
        try {
            Admin hbaseAdmin = _hbaseConnection.getAdmin();
            HBaseSchema.MetadataTableInfo info = _schema.metadataTableInfos[i];
            TableName name = info.getTableName();

            if (!hbaseAdmin.tableExists(name)) {
                hbaseAdmin.createTable(info.getDescriptor());
            }
            this.metaData[i] = new MetaData();
            this.metaData[i].table = _hbaseConnection.getTable(name);

            // set column counter
            byte[] value = Bytes.toBytes(0L);
            Put p = new Put(REFCOUNTER);
            p.addColumn(info.getColumnFamily(), info.getColumn(), value);
            metaData[i].table.checkAndPut(REFCOUNTER, info.getColumnFamily(), info.getColumn(), null, p);

        } catch (IOException e) {
            LOG.error("Could not assign metrics table", e);
        }
    }

    private void initializeMap(MetaDataIndex index) {
        int i = index.ordinal();
        metaData[i].map = new HashMap<String, Integer>();
        metaData[i].rmap = new HashMap<Integer, String>();

        Scan s = new Scan();
        byte[] columnFamily = _schema.metadataTableInfos[i].getColumnFamily();
        byte[] column = _schema.metadataTableInfos[i].getColumn();
        s.addColumn(columnFamily, column);

        try {
            ResultScanner scanner = metaData[i].table.getScanner(s);

            scanner.forEach((result) -> {
                byte[] key = result.getRow();
                byte[] value = result.getValue(columnFamily, column);

                String keyString = Bytes.toString(key);
                Integer valueInteger = Bytes.toInt(value);

                metaData[i].map.put(keyString, valueInteger);
                metaData[i].rmap.put(valueInteger, keyString);
            });

        } catch (IOException e) {
            LOG.error("Could not scan table", e);
        }
    }

    private Integer insertNewMapping(String keyStr, MetaDataIndex metaIndex) {

        int i = metaIndex.ordinal();
        MetaData meta = metaData[i];
        HBaseSchema.MetadataTableInfo info = _schema.metadataTableInfos[i];

        byte[] key = Bytes.toBytes(keyStr);
        byte[] columnFamily = info.getColumnFamily();
        byte[] column = info.getColumn();

        // check if exists in DB
        Get g = new Get(key);
        g.addColumn(columnFamily, column);

        try {
            Result result = meta.table.get(g);
            if (!result.isEmpty()) {
                byte[] columnValue = result.getValue(columnFamily, column);
                return Bytes.toInt(columnValue);
            }
        } catch (IOException e) {
            LOG.error("Could not get key ref", e);
            // TODO: continue or not?
        }

        // get ref counter
        long counter;
        Increment inc = new Increment(REFCOUNTER);
        inc.setReturnResults(true);

        try {
            counter = meta.table.incrementColumnValue(REFCOUNTER, columnFamily, column, 1);
        } catch (IOException e) {
            LOG.error("Could not get column ref", e);
            return null;
        }

        // set new
        try {
            Put p = new Put(key);
            byte[] value = Bytes.toBytes(counter);
            p.addColumn(columnFamily, column, value);
            meta.table.put(p);
        } catch (IOException e) {
            LOG.error("Could not create mapping");
            return null;
        }

        return (int) counter;

    }

    private Integer getRef(MetaDataIndex metaIndex, String key) {

        int i = metaIndex.ordinal();
        MetaData meta = metaData[i];
        Integer ref = null;

        if (!meta.map.containsKey(key)) {
            ref = insertNewMapping(key, metaIndex);
            meta.map.put(key, ref);
            meta.rmap.put(ref, key);
        }

        return ref;
    }


    public Put createPutOperation(Metric m) throws MetricException {

        HBaseSchema.MetricsTableInfo info = _schema.metricsTableInfo;

        boolean isAggregate = m.getCount() > 1;
        byte[] key = createKey(m);
        byte[] value = Bytes.toBytes(m.getValue());
        byte[] columnFamily = info.getColumnFamily();

        Put p = new Put(key);

        p.addColumn(columnFamily, info.getValueColumn(), value);

        if (isAggregate) {
            byte[] sum = Bytes.toBytes(m.getSum());
            byte[] count = Bytes.toBytes(m.getCount());
            byte[] min = Bytes.toBytes(m.getMin());
            byte[] max = Bytes.toBytes(m.getMax());

            p.addColumn(columnFamily, info.getSumColumn(), sum);
            p.addColumn(columnFamily, info.getCountColumn(), count);
            p.addColumn(columnFamily, info.getMinColumn(), min);
            p.addColumn(columnFamily, info.getMaxColumn(), max);
        }

        return p;
    }


    private byte[] createKey(Metric m) throws MetricException {
        Integer topoId = getRef(TOPOLOGY, m.getTopoIdStr());
        Integer streamId = getRef(STREAM, m.getStream());
        Integer hostId = getRef(HOST, m.getHost());
        Integer compId = getRef(COMP, m.getCompName());
        Integer metricNameId = getRef(METRICNAME, m.getMetricName());
        Integer executorId = getRef(EXECUTOR, m.getExecutor());

        ByteBuffer bb = ByteBuffer.allocate(41);

        try {
            bb.put(m.getAggLevel());
            bb.putInt(topoId);
            bb.putLong(m.getTimeStamp());
            bb.putInt(metricNameId);
            bb.putInt(compId);
            bb.putInt(executorId);
            bb.putInt(hostId);
            bb.putLong(m.getPort());
            bb.putInt(streamId);
        } catch (NullPointerException e) {
            throw new MetricException("Could not create metric key - null IDs " + e);
        }

        int length = bb.position();
        bb.position(0);

        byte[] key = new byte[length];
        bb.get(key, 0, length);

        return key;
    }

}
