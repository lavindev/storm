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


import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.mapred.RowCounter;
import org.apache.storm.generated.Window;
import org.apache.storm.utils.Time;
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

public class HBaseStore implements MetricStore {
    private final static Logger LOG = LoggerFactory.getLogger(HBaseStore.class);

    private final static String BASE_CONFIG_KEY = "storm.metrics2.store.HBaseStore";
    private final static String SCHEMA_KEY = BASE_CONFIG_KEY + ".schema";
    private final static String RETENTION_KEY = BASE_CONFIG_KEY + ".retention";
    private final static String RETENTION_UNIT_KEY = BASE_CONFIG_KEY + ".retention.units";
    private final static String HBASE_ROOT_DIR_KEY = BASE_CONFIG_KEY + ".hbase.root_dir";
    private final static String HBASE_METRICS_TABLE_KEY = BASE_CONFIG_KEY + ".hbase.metrics_table";
    private final static String ZOOKEEPER_SERVERS_KEY = "storm.zookeeper.servers";
    private final static String ZOOKEEPER_PORT_KEY = "storm.zookeeper.port";
    private final static String ZOOKEEPER_ROOT_KEY = "storm.zookeeper.root";

    private byte[] COLUMN_FAMILY;
    private byte[] COLUMN_VALUE;
    private byte[] COLUMN_COUNT;
    private byte[] COLUMN_SUM;
    private byte[] COLUMN_MIN;
    private byte[] COLUMN_MAX;

    private Connection _hbaseConnection = null;
    private Admin _hbaseAdmin = null;
    private Table _metricsTable = null;
    private HBaseSerializer _serializer = null;

    private byte _topoMetadataKey = (byte) 0;
    private byte _streamMetadataKey = (byte) 1;
    private byte _hostMetadataKey = (byte) 2;

    /**
     * Create HBase instance
     * using the configurations provided via the config map
     *
     * @param config Storm config map
     */
    @Override
    public void prepare(Map config) throws MetricException {

        // setup
        validateConfig(config);
        initializeSchema(config);
        Configuration hbaseConf = createHBaseConfiguration(config);
        HTableDescriptor tableDesc = createMetricsTableDescriptor(config);

        // connect
        try {
            this._hbaseConnection = ConnectionFactory.createConnection(hbaseConf);
            this._hbaseAdmin = _hbaseConnection.getAdmin();
            if (!_hbaseAdmin.tableExists(tableDesc.getTableName())) {
                _hbaseAdmin.createTable(tableDesc);
            }
            this._metricsTable = _hbaseConnection.getTable(tableDesc.getTableName());
            // TODO: pass values
            this._serializer = new HBaseSerializer();
        } catch (IOException e) {
            throw new MetricException("HBase connection error " + e);
        }

        // restore metadata
        LOG.info("Restoring metadata");
        scanKV(_serializer.makeKey(_topoMetadataKey, null), (key, value) -> {
            return _serializer.putToTopoMap(key, value);
        });
        scanKV(_serializer.makeKey(_streamMetadataKey, null), (key, value) -> {
            return _serializer.putToStreamMap(key, value);
        });
        scanKV(_serializer.makeKey(_hostMetadataKey, null), (key, value) -> {
            return _serializer.putToHostMap(key, value);
        });

    }

    private Configuration createHBaseConfiguration(Map config) {
        // TODO: read from config, fix cast?
        Configuration conf = HBaseConfiguration.create();

        String hbaseRootDir = (String) config.get(HBASE_ROOT_DIR_KEY);
        String zookeeperQuorum = String.join(";", (List) config.get(ZOOKEEPER_SERVERS_KEY));
        String zookeeperRoot = (String) config.get(ZOOKEEPER_ROOT_KEY);
        int zookeeperPort = (int) config.get(ZOOKEEPER_PORT_KEY);

        conf.set(HConstants.HBASE_DIR, hbaseRootDir);
        conf.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum);
        conf.set(HConstants.ZOOKEEPER_DATA_DIR, zookeeperRoot);
        conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zookeeperPort);

        return conf;
    }

    private HTableDescriptor createMetricsTableDescriptor(Map config) {

        String name = (String) config.get(HBASE_METRICS_TABLE_KEY);
        TableName tableName = TableName.valueOf(name);
        HColumnDescriptor columnFamily = new HColumnDescriptor(COLUMN_FAMILY);

        HTableDescriptor descriptor = new HTableDescriptor(tableName).addFamily(columnFamily);

        return descriptor;
    }

    private void initializeSchema(Map config) {
        // TODO: config
        this.COLUMN_FAMILY = Bytes.toBytes("c");
        this.COLUMN_VALUE = Bytes.toBytes("v");
        this.COLUMN_COUNT = Bytes.toBytes("c");
        this.COLUMN_SUM = Bytes.toBytes("s");
        this.COLUMN_MIN = Bytes.toBytes("i");
        this.COLUMN_MAX = Bytes.toBytes("a");
    }


    private void validateConfig(Map config) throws MetricException {
        // TODO: check values, fix error strings

        if (!(config.containsKey(HBASE_ROOT_DIR_KEY))) {
            throw new MetricException("Need HBase root dir");
        }
        if (!(config.containsKey(SCHEMA_KEY))) {
            throw new MetricException("Need hbase schema - compact or expanded");
        }
        if (!(config.containsKey(HBASE_METRICS_TABLE_KEY))) {
            throw new MetricException("Need HBase metrics table");
        }
        if (!(config.containsKey(RETENTION_KEY) && config.containsKey(RETENTION_UNIT_KEY))) {
            throw new MetricException("Need retention value/units");
        }
        if (!(config.containsKey(ZOOKEEPER_ROOT_KEY))) {
            throw new MetricException("Need ZooKeeper Root/Data directory");
        }
        if (!(config.containsKey(ZOOKEEPER_SERVERS_KEY))) {
            throw new MetricException("Need ZooKeeper servers list");
        }
        if (!(config.containsKey(ZOOKEEPER_PORT_KEY))) {
            throw new MetricException("Need ZooKeeper port");
        }
    }

    /**
     * Stores metrics in the store
     *
     * @param m Metric to store
     */
    @Override
    public void insert(Metric m) {

        LOG.info("inserting {}", m.toString());

        // load metadata
        try {
            if (!_serializer.metaInitialized(m)) {
                _serializer.deserializeMeta(m.getTopoIdStr(), get(_serializer.metadataKey(m)));
            }
        } catch (IOException e) {
            LOG.error("Could not load metadata", e);
            return;
        }

        // add mapping in memory
        Integer topoId = _serializer.getTopoId(m.getTopoIdStr());
        Integer hostId = _serializer.getHostId(m.getHost());
        Integer streamId = _serializer.getStreamId(m.getStream());

        // create new batch op
        HBaseStoreBatchManager batch = new HBaseStoreBatchManager(_metricsTable)
                .withColumnFamily(COLUMN_FAMILY)
                .withColumn(COLUMN_VALUE);

        // persist metadata
        try {
            batch.put(_serializer.makeKey(_topoMetadataKey, topoId), m.getTopoIdStr().getBytes("UTF-8"));
            batch.put(_serializer.makeKey(_hostMetadataKey, hostId), m.getHost().getBytes("UTF-8"));
            batch.put(_serializer.makeKey(_streamMetadataKey, streamId), m.getStream().getBytes("UTF-8"));
        } catch (java.io.UnsupportedEncodingException ex) {
            LOG.error("Unsupported encoding!", ex);
            return;
        }

        HBaseSerializer.SerializationResult sr = _serializer.serialize(m);
        batch.put(sr.metricKey, sr.metricValue);

        if (sr.metaTopoValue != null) {
            batch.put(_serializer.metadataKey(m), sr.metaTopoValue);
        }

        try {
            batch.execute(true);
        } catch (IOException | InterruptedException e) {
            LOG.error("Could not insert into HBase", e);
        }
    }

    /**
     * Scans all metrics in the store
     *
     * @return void
     */
    @Override
    public void scan(IAggregator agg) {
        for (String topoId : _serializer.getTopoIds()) {
            LOG.debug("full scanning for topology {}", topoId);
            HashMap<String, Object> settings = new HashMap<String, Object>();
            settings.put(StringKeywords.topoId, topoId);
            scan(settings, agg);
        }
    }

    /**
     * Implements scan method of the Metrics Store, scans all metrics with settings in the store
     * Will try to search the fastest way possible
     *
     * @param settings map of settings to search by
     * @param agg      Callback fn, called as we are scanning through store
     * @return void
     */
    @Override
    public void scan(HashMap<String, Object> settings, IAggregator agg) {
        // load metadata
        // TODO: make function that can take the settings and decide
        // whether we have enough info to find the db
        // rather than construct a metric
        Metric m = new Metric();
        String topoIdStr = (String) settings.get(StringKeywords.topoId);
        m.setTopoIdStr(topoIdStr);

        try {
            if (!_serializer.metaInitialized(m)) {
                _serializer.deserializeMeta(m.getTopoIdStr(), get(_serializer.metadataKey(m)));
            }
        } catch (IOException ex) {
            LOG.error("Error loading metadata", ex);
        }

        if (settings.containsKey(StringKeywords.timeRangeSet)){
            Set<TimeRange> timeRanges = (Set<TimeRange>) settings.get(StringKeywords.timeRangeSet);
            System.out.println(settings.toString());
        }

        byte[] prefix = _serializer.createPrefix(settings);
        if (prefix != null) {
            scan(prefix, settings, agg);
            return;
        }
        LOG.error("Couldn't obtain prefix");
    }


    public byte[] get(byte[] key) throws IOException {
        Get g = new Get(key);
        Result result = _metricsTable.get(g);
        return result.getValue(COLUMN_FAMILY, COLUMN_VALUE);
    }

    public void put(byte[] key, byte[] value) throws IOException {
        Put p = new Put(key);
        p.addColumn(COLUMN_FAMILY, COLUMN_VALUE, value);
        _metricsTable.put(p);
    }


    /**
     * Implements scan method of the Metrics Store, scans all metrics with prefix in the store
     *
     * @param prefix   prefix to query in store
     * @param settings search settings
     * @return List<String> metrics in store
     */
    private void scanKV(byte[] prefix, IScanCallback fn) {
        String prefixStr = Hex.encodeHexString(prefix);
        LOG.info("Prefix kv scan with {} and length {}", prefixStr, prefix.length);

        Scan scan = new Scan();
        scan.setRowPrefixFilter(prefix);
        long startTime = System.nanoTime();
        long numRecords = 0;
        long numScannedRecords = 0;

        try {
            ResultScanner scanner = _metricsTable.getScanner(scan);

            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                byte[] key = result.getRow();
                byte[] value = result.getValue(COLUMN_FAMILY, COLUMN_VALUE);
                // put metadata to _serializer maps
                if (!fn.cb(key, value)) {
                    // if cb returns false, we are done with this section of rows
                    break;
                }
                numRecords++;
                numScannedRecords++;
            }

            scanner.close();
        } catch (IOException e) {
            LOG.error("Could not scan HBase", e);
        }
        LOG.info("prefix scan complete for {} with {} records and {} scanned total in {} ms",
                prefixStr, numRecords, numScannedRecords, (System.nanoTime() - startTime) / 1000000);
    }

    private void scan(byte[] prefix, HashMap<String, Object> settings, IAggregator agg) {
        String prefixStr = Hex.encodeHexString(prefix);
        LOG.info("Prefix scan with {} and length {}", prefixStr, prefix.length);

        Scan scan = new Scan();
        scan.addColumn(COLUMN_FAMILY, COLUMN_VALUE);
        scan.setRowPrefixFilter(prefix);
        long startTime = System.nanoTime();
        long numRecords = 0;
        long numScannedRecords = 0;

        try {

            ResultScanner scanner = _metricsTable.getScanner(scan);
            for (Result result = scanner.next(); result != null; result = scanner.next()) {

                byte[] key = result.getRow();
                byte[] value = result.getValue(COLUMN_FAMILY, COLUMN_VALUE);

                Metric metric = _serializer.deserialize(key);
                if (metric == null) {
                    // if we can't deserialize (e.g. key type is metadata)
                    // we should be done
                    break;
                }
                LOG.debug("Scanning key: {}", metric.toString());

                Set<TimeRange> timeRanges = checkRequiredSettings(metric, settings);
                numScannedRecords++;
                if (timeRanges != null) {
                    boolean include = checkMetric(metric, settings);
                    if (include) {
                        LOG.debug("Match key: {}", Hex.encodeHexString(key));
                        numRecords++;
                        if (!_serializer.metaInitialized(metric)) {
                            _serializer.deserializeMeta(metric.getTopoIdStr(), _serializer.metadataKey(metric));
                        }
                        _serializer.populate(metric, value);
                        agg.agg(metric, timeRanges);
                    }
                } else {
                    // if we don't find in required settings, no need to continue
                    break;
                }
            }

            scanner.close();
        } catch (IOException e) {
            LOG.error("Could not scan HBase", e);
        }

        LOG.info("prefix scan complete for {} with {} records and {} scanned total in {} ms",
                prefixStr, numRecords, numScannedRecords, (System.nanoTime() - startTime) / 1000000);

    }

    // get by matching a key exactly
    @Override
    public boolean populateValue(Metric metric) {
        try {
            // load metadata
            if (!_serializer.metaInitialized(metric)) {
                _serializer.deserializeMeta(metric.getTopoIdStr(), get(_serializer.metadataKey(metric)));
            }

            HBaseSerializer.SerializationResult sr = _serializer.serialize(metric);

            byte[] value = get(sr.metricKey);
            _serializer.populate(metric, value);
            return value != null;
        } catch (IOException ex) {
            LOG.error("Exception getting value:", ex);
            return false;
        }
    }

    // remove things matching settings, kind of like a scan but much scarier
    @Override
    public void remove(HashMap<String, Object> settings) {
        // for each key we match, remove it
        scan(settings, (metric, timeRanges) -> {
            remove(metric);
        });
    }

    public void remove(Metric keyToRemove) {
        try {
            HBaseSerializer.SerializationResult result = _serializer.serialize(keyToRemove);
            Delete d = new Delete(result.metricKey);
            _metricsTable.delete(d);
        } catch (IOException ex) {
            LOG.error("Exception while removing {}", keyToRemove);
        }
    }

    /**
     * Implements configuration validation of Metrics Store, validates storm configuration for Metrics Store
     *
     * @param possibleKey key to check
     * @param settings    search settings
     * @throws MetricException if there is a missing required configuration or if the store does not exist but
     *                         the config specifies not to create the store
     */
    private boolean checkMetric(Metric possibleKey, HashMap<String, Object> settings) {
        LOG.info("Checking {}", possibleKey);
        if (settings.containsKey(StringKeywords.component) &&
                !possibleKey.getCompName().equals(settings.get(StringKeywords.component))) {
            LOG.info("Not the right component {}", possibleKey.getCompName());
            return false;
        } else if (settings.containsKey(StringKeywords.metricSet) &&
                !((HashSet<String>) settings.get(StringKeywords.metricSet)).contains(possibleKey.getMetricName())) {
            LOG.info("Not the right metric name {}", possibleKey.getMetricName());
            return false;
        }
        return true;
    }

    private Set<TimeRange> checkRequiredSettings(Metric possibleKey, HashMap<String, Object> settings) {

        Integer aggValue = (Integer) settings.get(StringKeywords.aggLevel);
        byte aggLevel = (aggValue != null) ? aggValue.byteValue() : (byte) 0;

        LOG.info("compare agg level: {} {}", possibleKey.getAggLevel(), aggLevel);

        if (settings.containsKey(StringKeywords.aggLevel) &&
                (possibleKey.getAggLevel() == null ||
                        !possibleKey.getAggLevel().equals(((Integer) settings.get(StringKeywords.aggLevel)).byteValue()))) {
            LOG.info("DOES NOT MATCH agg level: {} {}", possibleKey.getAggLevel(), ((Integer) settings.get(StringKeywords.aggLevel)).byteValue());
            return null;
        } else if (settings.containsKey(StringKeywords.topoId) &&
                !possibleKey.getTopoIdStr().equals(settings.get(StringKeywords.topoId))) {
            LOG.info("DOES NOT MATCH topo {} {}", possibleKey.getTopoIdStr(), settings.get(StringKeywords.topoId));
            return null;
        } else if (settings.containsKey(StringKeywords.timeRangeSet)) {
            Set<TimeRange> timeRangeSet = (Set<TimeRange>) settings.get(StringKeywords.timeRangeSet);
            Set<TimeRange> matchedTimeRanges = new HashSet<TimeRange>();
            for (TimeRange tr : timeRangeSet) {
                Long tstamp = possibleKey.getTimeStamp();
                if (tr.contains(tstamp)) {
                    matchedTimeRanges.add(tr);
                }
            }
            if (matchedTimeRanges.size() == 0) {
                LOG.info("DOES NOT MATCH time ranges");
            }
            return matchedTimeRanges.size() > 0 ? matchedTimeRanges : null;
        }
        HashSet<TimeRange> ret = new HashSet<TimeRange>();
        ret.add(new TimeRange(0L, Long.MAX_VALUE, Window.ALL));
        return ret;
    }


    public void close() {
        try {
            if (_metricsTable != null) _metricsTable.close();
            if (_hbaseConnection != null) _hbaseConnection.close();
        } catch (IOException e) {
            LOG.warn("Could not close table/connection ", e);
        }
    }

}
