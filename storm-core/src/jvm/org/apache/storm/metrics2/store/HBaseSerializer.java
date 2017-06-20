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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.generated.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.storm.metrics2.store.HBaseMetadataIndex.*;

public abstract class HBaseSerializer {
    private final static Logger LOG = LoggerFactory.getLogger(HBaseSerializer.class);

    private class MetaData {
        public HashMap<String, Integer> map;
        public HashMap<Integer, String> rmap;
        public HTableInterface table;
    }


    protected HConnection _hbaseConnection;
    protected HBaseSchema _schema;

    private MetaData[] metaData;

    public static HBaseSerializer createSerializer(HConnection hbaseConnection, HBaseSchema schema)
            throws MetricException {

        HBaseSchemaType type = schema.getSchemaType();
        try {
            Class<?> clazz = Class.forName(type.getClassName());
            Constructor<?> ctor = clazz.getConstructor(HConnection.class, HBaseSchema.class);
            return (HBaseSerializer) ctor.newInstance(hbaseConnection, schema);
        } catch (Exception e) {
            throw new MetricException("Could not initialize serializer - " + e);
        }
    }

    public HBaseSerializer(HConnection hbaseConnection, HBaseSchema schema) {

        // TODO: fix configuration lookup
        this._hbaseConnection = hbaseConnection;
        Configuration conf = hbaseConnection.getConfiguration();
        this._schema = schema;
        this.metaData = new MetaData[HBaseMetadataIndex.count()];

        for (HBaseMetadataIndex index : HBaseMetadataIndex.values()) {
            assignMetaDataTable(index);
            initializeMap(index);
        }

    }

    private void assignMetaDataTable(HBaseMetadataIndex meta) {
        int i = meta.getIndex();
        try {
            HBaseAdmin hbaseAdmin = new HBaseAdmin(_hbaseConnection);
            HBaseSchema.MetadataTableInfo info = _schema.metadataTableInfos[i];
            TableName name = info.getTableName();

            if (!hbaseAdmin.tableExists(name)) {
                hbaseAdmin.createTable(info.getDescriptor());
            }
            this.metaData[i] = new MetaData();
            this.metaData[i].table = _hbaseConnection.getTable(name);

            // set column counter
            byte[] refcounter = info.getRefcounter();
            byte[] value = Bytes.toBytes(0L);
            byte[] cf = info.getColumnFamily();
            byte[] column = info.getColumn();
            Put p = new Put(refcounter);
            p.add(cf, column, value);
            metaData[i].table.checkAndPut(refcounter, cf, column, null, p);

        } catch (IOException e) {
            LOG.error("Could not assign metrics table", e);
        }
    }

    private void initializeMap(HBaseMetadataIndex meta) {
        int i = meta.getIndex();
        if (metaData[i].map == null)
            metaData[i].map = new HashMap<String, Integer>();
        if (metaData[i].rmap == null)
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

                // NOTE: key REFCOUNTER stores a long
                // valueInteger returns int from upper 4 bytes of stored long
                // Thus, REFCOUNTER value in local map will always be incorrect
                String keyString = Bytes.toString(key);
                Integer valueInteger = Bytes.toInt(value);

                metaData[i].map.put(keyString, valueInteger);
                metaData[i].rmap.put(valueInteger, keyString);
            });

        } catch (IOException e) {
            LOG.error("Could not scan table", e);
        }
    }

    private Integer checkExistingMapping(String keyStr, HBaseMetadataIndex metaIndex) {

        int i = metaIndex.getIndex();
        MetaData meta = metaData[i];
        HBaseSchema.MetadataTableInfo info = _schema.metadataTableInfos[i];

        byte[] key = Bytes.toBytes(keyStr);
        byte[] columnFamily = info.getColumnFamily();
        byte[] column = info.getColumn();

        Get g = new Get(key);
        g.addColumn(columnFamily, column);

        try {
            Result result = meta.table.get(g);
            if (!result.isEmpty()) {
                byte[] columnValue = result.getValue(columnFamily, column);
                return Bytes.toInt(columnValue);
            }
        } catch (IOException e) {
            LOG.error("Could not get ref ", e);
        }

        return null;
    }

    private Integer insertNewMapping(String keyStr, HBaseMetadataIndex metaIndex) {

        int i = metaIndex.getIndex();
        MetaData meta = metaData[i];
        HBaseSchema.MetadataTableInfo info = _schema.metadataTableInfos[i];

        byte[] key = Bytes.toBytes(keyStr);
        byte[] columnFamily = info.getColumnFamily();
        byte[] column = info.getColumn();
        byte[] refcounter = info.getRefcounter();

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
        Integer counter;
        Increment inc = new Increment(refcounter);
        //inc.setReturnResults(true);

        try {
            counter = (int) meta.table.incrementColumnValue(refcounter, columnFamily, column, 1);
        } catch (IOException e) {
            LOG.error("Could not get column ref", e);
            return null;
        }

        // set new
        try {
            Put p = new Put(key);
            byte[] value = Bytes.toBytes(counter);
            p.add(columnFamily, column, value);
            meta.table.put(p);
        } catch (IOException e) {
            LOG.error("Could not create mapping");
            return null;
        }

        return counter;

    }

    private Integer getRef(HBaseMetadataIndex metaIndex, String key) {

        int i = metaIndex.getIndex();
        MetaData meta = metaData[i];

        Integer ref = meta.map.get(key);

        if (ref == null && key != null) {
            ref = checkExistingMapping(key, metaIndex);
            if (ref == null)
                ref = insertNewMapping(key, metaIndex);
            meta.map.put(key, ref);
            meta.rmap.put(ref, key);
        }

        return ref;
    }

    private String getReverseRef(HBaseMetadataIndex metaIndex, Integer ref) {

        int i = metaIndex.getIndex();
        MetaData meta = metaData[i];

        if (!meta.rmap.containsKey(ref) && ref != null) {
            // reload map from store
            initializeMap(metaIndex);
        }

        return meta.rmap.get(ref);
    }

    public abstract Put createPutOperation(Metric m) throws MetricException;

    public byte[] createKey(Metric m) throws MetricException {
        Integer topoId = getRef(TOPOLOGY, m.getTopoIdStr());
        Integer streamId = getRef(STREAM, m.getStream());
        Integer hostId = getRef(HOST, m.getHost());
        Integer compId = getRef(COMP, m.getCompName());
        Integer metricNameId = getRef(METRICNAME, m.getMetricName());
        Integer executorId = getRef(EXECUTOR, m.getExecutor());

        ByteBuffer bb = ByteBuffer.allocate(33);

        try {
            bb.put(m.getAggLevel());
            bb.putInt(topoId);
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

    public boolean populateMetricKey(Metric m, Result result) {

        byte[] key = result.getRow();
        long timeStamp = result.rawCells()[0].getTimestamp();

        ByteBuffer bb = ByteBuffer.allocate(33).put(key);
        bb.rewind();

        Byte aggLevel = bb.get();
        Integer topoId = bb.getInt();
        Integer metricNameId = bb.getInt();
        Integer compId = bb.getInt();
        Integer executorId = bb.getInt();
        Integer hostId = bb.getInt();
        long port = bb.getLong();
        Integer streamId = bb.getInt();

        String topoIdStr = getReverseRef(TOPOLOGY, topoId);
        String metricNameStr = getReverseRef(METRICNAME, metricNameId);
        String compIdStr = getReverseRef(COMP, compId);
        String execIdStr = getReverseRef(EXECUTOR, executorId);
        String hostIdStr = getReverseRef(HOST, hostId);
        String streamIdStr = getReverseRef(STREAM, streamId);

        m.setAggLevel(aggLevel);
        m.setTopoIdStr(topoIdStr);
        m.setTimeStamp(timeStamp);
        m.setMetricName(metricNameStr);
        m.setCompName(compIdStr);
        m.setExecutor(execIdStr);
        m.setHost(hostIdStr);
        m.setPort(port);
        m.setStream(streamIdStr);

        return true;
    }

    public abstract boolean populateMetricValue(Metric m, Result result);

    public List<Scan> createScanOperation(HashMap<String, Object> settings) {

        // grab values from map
        Integer aggLevel = (Integer) settings.get(StringKeywords.aggLevel);
        String topoIdStr = (String) settings.get(StringKeywords.topoId);
        String compIdStr = (String) settings.get(StringKeywords.component);
        String execIdStr = (String) settings.get(StringKeywords.executor);
        String hostIdStr = (String) settings.get(StringKeywords.host);
        String portStr = (String) settings.get(StringKeywords.port);
        String streamIdStr = (String) settings.get(StringKeywords.stream);
        HashSet<String> metricStrSet = (HashSet<String>) settings.get(StringKeywords.metricSet);
        Set<TimeRange> timeRangeSet = (Set<TimeRange>) settings.get(StringKeywords.timeRangeSet);

        // convert strings to Integer references
        Integer topoId = getRef(TOPOLOGY, topoIdStr);
        Integer compId = getRef(COMP, compIdStr);
        Integer execId = getRef(EXECUTOR, execIdStr);
        Integer hostId = getRef(HOST, hostIdStr);
        Integer streamId = getRef(STREAM, streamIdStr);
        Long port = (portStr == null) ? null : Long.parseLong(portStr);

        HashSet<Integer> metricIds = null;

        if (metricStrSet != null) {
            metricIds = new HashSet<>(metricStrSet.size());
            for (String s : metricStrSet) {
                Integer ref = getRef(METRICNAME, s);
                if (ref != null)
                    metricIds.add(ref);
                else
                    LOG.error("Could not lookup {} reference", s);
            }
            LOG.info("{}-{}", metricStrSet, metricIds);
        }

        HBaseStoreScan scan = new HBaseStoreScan()
                .withAggLevel(aggLevel)
                .withTopoId(topoId)
                .withTimeRange(timeRangeSet)
                .withMetricSet(metricIds)
                .withCompId(compId)
                .withExecutorId(execId)
                .withHostId(hostId)
                .withPort(port)
                .withStreamId(streamId);

        return scan.getScanList();
    }

    public Metric deserializeMetric(Result result) {
        Metric m = new Metric();
        populateMetricKey(m, result);
        populateMetricValue(m, result);
        return m;
    }

    private class HBaseStoreScan {

        private final static int PRE_START_OFFSET = 0;
        private final static int PRE_LENGTH = 5;
        private final static int POST_START_OFFSET = 9;
        private final static int POST_LENGTH = 24;
        private final static int METRIC_OFFSET = PRE_LENGTH;
        private final static int METRIC_LENGTH = 4;


        private ArrayList<Scan> scanList;
        private ByteBuffer pre;
        private ByteBuffer post;
        private int prefixLength;
        private HashSet<Integer> metricIds;
        private Set<TimeRange> timeRangeSet;

        HBaseStoreScan() {
            pre = ByteBuffer.allocate(PRE_LENGTH);
            post = ByteBuffer.allocate(POST_LENGTH);
            prefixLength = 0;
        }

        HBaseStoreScan withAggLevel(Integer aggLevel) {
            if (aggLevel != null) {
                pre.put(aggLevel.byteValue());
                prefixLength = 1;
            }
            return this;
        }

        HBaseStoreScan withTopoId(Integer topoId) {
            if (topoId != null) {
                pre.putInt(topoId);
                prefixLength = 5;
            }
            return this;
        }

        HBaseStoreScan withTimeRange(Set<TimeRange> timeRangeSet) {
            if (timeRangeSet != null) {
                this.timeRangeSet = timeRangeSet;

                // debug
                for (TimeRange timeRange : timeRangeSet) {
                    String windowStr = timeRange.window == Window.ONE_DAY ? "ONE_DAY" :
                            timeRange.window == Window.TEN_MIN ? "TEN_MIN" :
                                    timeRange.window == Window.THREE_HR ? "THREE_HR" : "ALL";
                    LOG.info("Window = {}, start = {}, end = {}", windowStr, timeRange.startTime, timeRange.endTime);
                }

            }
            return this;
        }

        HBaseStoreScan withMetricSet(HashSet<Integer> metricIds) {
            if (metricIds != null && !metricIds.isEmpty()) {
                this.metricIds = metricIds;
                prefixLength = 9;
            }
            return this;
        }

        HBaseStoreScan withCompId(Integer compId) {
            if (compId != null) {
                post.putInt(compId);
                prefixLength = 13;
            }
            return this;
        }

        HBaseStoreScan withExecutorId(Integer executorId) {
            if (executorId != null) {
                post.putInt(executorId);
                prefixLength = 17;
            }
            return this;
        }

        HBaseStoreScan withHostId(Integer hostId) {
            if (hostId != null) {
                post.putInt(hostId);
                prefixLength = 21;
            }
            return this;
        }

        HBaseStoreScan withPort(Long port) {
            if (port != null) {
                post.putLong(port);
                prefixLength = 29;
            }
            return this;
        }

        HBaseStoreScan withStreamId(Integer streamId) {
            if (streamId != null) {
                post.putInt(streamId);
                prefixLength = 33;
            }
            return this;
        }

        List<Scan> getScanList() {
            if (scanList == null)
                generateScanList();
            return scanList;
        }

        private void generateScanList() {

            scanList = new ArrayList<>();

            // create buffer without metricId
            byte[] prefixArray = new byte[prefixLength];
            int byteBufferLength;
            if (prefixLength > PRE_START_OFFSET) {
                byteBufferLength = pre.position();
                pre.position(0);
                pre.get(prefixArray, PRE_START_OFFSET, byteBufferLength);
            }
            if (prefixLength > POST_START_OFFSET) {
                byteBufferLength = post.position();
                post.position(0);
                post.get(prefixArray, POST_START_OFFSET, byteBufferLength);
            }

            byte[] metricBytes;
            if (metricIds != null) {
                for (Integer metricId : metricIds) {
                    metricBytes = Bytes.toBytes(metricId);
                    System.arraycopy(metricBytes, 0, prefixArray, METRIC_OFFSET, METRIC_LENGTH);
                    createNewScans(prefixArray.clone());
                }
            } else {
                createNewScans(prefixArray.clone());
            }

        }

        private void createNewScans(byte[] prefix) {

            Scan s = new Scan();
            setRowPrefixFilter(s, prefix);

            if (timeRangeSet != null) {

                long start, end;
                for (TimeRange timeRange : timeRangeSet) {
                    start = timeRange.startTime != null ? timeRange.startTime : Long.MIN_VALUE;
                    end = timeRange.endTime != null ? timeRange.endTime : Long.MAX_VALUE - 1;

                    if (end < start) {
                        start ^= end;
                        end ^= start;
                        start ^= end;
                    }

                    try {
                        s.setTimeRange(start, end);
                        LOG.info("Creating scan with prefix {} between {} - {}", Bytes.toStringBinary(prefix), start, end);
                        scanList.add(new Scan(s));
                    } catch (IOException e) {
                        LOG.error("Could not create scan min = {} max = {}", start, end, e);
                    }
                }

            } else {
                LOG.info("Creating scan with prefix {}", Bytes.toStringBinary(prefix));
                scanList.add(s);
            }


        }

        /* from newer hbase */
        private void setRowPrefixFilter(Scan s, byte[] rowPrefix) {
            if (rowPrefix == null) {
                s.setStartRow(HConstants.EMPTY_START_ROW);
                s.setStopRow(HConstants.EMPTY_END_ROW);
            } else {
                s.setStartRow(rowPrefix);
                s.setStopRow(calculateTheClosestNextRowKeyForPrefix(rowPrefix));
            }
        }

        private byte[] calculateTheClosestNextRowKeyForPrefix(byte[] rowKeyPrefix) {
            // Essentially we are treating it like an 'unsigned very very long' and doing +1 manually.
            // Search for the place where the trailing 0xFFs start
            int offset = rowKeyPrefix.length;
            while (offset > 0) {
                if (rowKeyPrefix[offset - 1] != (byte) 0xFF) {
                    break;
                }
                offset--;
            }

            if (offset == 0) {
                // We got an 0xFFFF... (only FFs) stopRow value which is
                // the last possible prefix before the end of the table.
                // So set it to stop at the 'end of the table'
                return HConstants.EMPTY_END_ROW;
            }

            // Copy the right length of the original
            byte[] newStopRow = Arrays.copyOfRange(rowKeyPrefix, 0, offset);
            // And increment the last one
            newStopRow[newStopRow.length - 1]++;
            return newStopRow;
        }

    }

}
