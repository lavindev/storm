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

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HConstants;

public class HBaseStore implements MetricStore {
    private final static Logger LOG = LoggerFactory.getLogger(HBaseStore.class);

    private final static String BASE_CONFIG_KEY = "storm.metrics2.store.HBaseStore";
    private final static String RETENTION_KEY = BASE_CONFIG_KEY + ".retention";
    private final static String RETENTION_UNIT_KEY = BASE_CONFIG_KEY + ".retention.units";
    private final static String HBASE_ROOT_DIR_KEY = BASE_CONFIG_KEY + ".hbase.root_dir";
    private final static String ZOOKEEPER_SERVERS_KEY = "storm.zookeeper.servers";
    private final static String ZOOKEEPER_PORT_KEY = "storm.zookeeper.port";
    private final static String ZOOKEEPER_ROOT_KEY = "storm.zookeeper.root";
    private final static String HBASE_META_TABLE_KEY = BASE_CONFIG_KEY + ".hbase.metrics_table";
    private final static String HBASE_META_CF_KEY = BASE_CONFIG_KEY + ".hbase.meta_cf";
    private final static String HBASE_META_COL_KEY = BASE_CONFIG_KEY + ".hbase.meta_column";
    private final static String HBASE_METRICS_TABLE_KEY = BASE_CONFIG_KEY + ".hbase.metrics_table";
    private final static String HBASE_METRICS_CF_KEY = BASE_CONFIG_KEY + ".hbase.metrics_cf";
    private final static String HBASE_METRICS_VALUE_COL_KEY = BASE_CONFIG_KEY + ".hbase.metrics_value_column";
    private final static String HBASE_METRICS_COUNT_COL_KEY = BASE_CONFIG_KEY + ".hbase.metrics_count_column";
    private final static String HBASE_METRICS_SUM_COL_KEY = BASE_CONFIG_KEY + ".hbase.metrics_sum_column";
    private final static String HBASE_METRICS_MIN_COL_KEY = BASE_CONFIG_KEY + ".hbase.metrics_min_column";
    private final static String HBASE_METRICS_MAX_COL_KEY = BASE_CONFIG_KEY + ".hbase.metrics_max_column";

    private Connection _hbaseConnection;
    private HBaseSchema _schema;
    private HBaseSerializer _serializer;
    private Table _metricsTable;

    /**
     * Create HBase instance
     * using the configurations provided via the config map
     *
     * @param config Storm config map
     */
    @Override
    public void prepare(Map config) throws MetricException {

        validateConfig(config);

        this._schema = new HBaseSchema(config);
        Configuration hbaseConf = createHBaseConfiguration(config);

        try {
            this._hbaseConnection = ConnectionFactory.createConnection(hbaseConf);
            Admin hbaseAdmin = _hbaseConnection.getAdmin();
            TableName name = _schema.metricsTableInfo.getTableName();
            HTableDescriptor descriptor = _schema.metricsTableInfo.getDescriptor();

            if (!hbaseAdmin.tableExists(name)) {
                hbaseAdmin.createTable(descriptor);
            }

            this._metricsTable = _hbaseConnection.getTable(_schema.metricsTableInfo.getTableName());
            this._serializer = new HBaseSerializer(_hbaseConnection, _schema);
        } catch (IOException e) {
            throw new MetricException("Could not connect to hbase " + e);
        }

    }

    private void validateConfig(Map config) throws MetricException {
        // TODO: add rest of validation

        if (!(config.containsKey(HBASE_ROOT_DIR_KEY))) {
            throw new MetricException("Need HBase root dir");
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


    /**
     * Stores metrics in the store
     *
     * @param m Metric to store
     */
    @Override
    public void insert(Metric m) {
        try {
            Put p = _serializer.createPutOperation(m);
            _metricsTable.put(p);
        } catch (MetricException | IOException e) {
            LOG.error("Could not insert metric ", e);
        }
    }

    /**
     * Scans all metrics in the store
     *
     * @param agg
     * @return void
     */
    @Override
    public void scan(IAggregator agg) {
        HashMap<String, Object> settings = new HashMap<String, Object>();
        scan(settings, agg);
    }

    /**
     * Implements scan method of the Metrics Store, scans all metrics with settings in the store
     * Will try to search the fastest way possible
     *
     * @param settings map of settings to search by
     * @param agg
     * @return List<Double> metrics in store
     */
    @Override
    public void scan(HashMap<String, Object> settings, IAggregator agg) {

        List<Scan> scanList = _serializer.createScanOperation(settings);

        scanList.forEach(s -> {

            int numRecordsScanned = 0;
            ResultScanner scanner = null;
            try {
                scanner = _metricsTable.getScanner(s);
            } catch (IOException e) {
                LOG.error("Could not scan metrics table ", e);
                return;
            }

            for (Result result : scanner) {
                ++numRecordsScanned;
                Metric metric = _serializer.deserializeMetric(result);
                Set<TimeRange> timeRanges = getTimeRanges(metric, settings);

                agg.agg(metric, timeRanges);
            }
            LOG.info("Scanned {} records", numRecordsScanned);
            scanner.close();
        });
    }

    private Set<TimeRange> getTimeRanges(Metric m, HashMap<String, Object> settings) {

        Set<TimeRange> timeRangeSet = (Set<TimeRange>) settings.get(StringKeywords.timeRangeSet);

        if (timeRangeSet == null) {
            return null;
        }

        Set<TimeRange> matchedTimeRanges = new HashSet<TimeRange>();

        for (TimeRange timeRange : timeRangeSet) {
            Long metricTimeStamp = m.getTimeStamp();
            if (timeRange.contains(metricTimeStamp))
                matchedTimeRanges.add(timeRange);
        }

        return matchedTimeRanges;
    }


    @Override
    public boolean populateValue(Metric metric) {

        byte[] key;

        try {
            key = _serializer.createKey(metric);
        } catch (MetricException e) {
            LOG.error("Bad metric passed to populateValue " + e);
            return false;
        }

        try {
            Get g = new Get(key);
            Result result = _metricsTable.get(g);
            return _serializer.populateMetricValue(metric, result);
        } catch (IOException e) {
            LOG.error("Could not read from database ", e);
            return false;
        }

    }

    @Override
    public void remove(HashMap<String, Object> settings) {

        ArrayList<Row> metricsToRemove = new ArrayList<>();

        scan(settings, (metric, timeRanges) -> {
            try {
                byte[] key = _serializer.createKey(metric);
                Delete d = new Delete(key);
                metricsToRemove.add(d);
            } catch (MetricException e) {
                LOG.error("Could not create key ", e);
            }
        });

        Result[] results = new Result[metricsToRemove.size()];

        try {
            _metricsTable.batch(metricsToRemove, results);
        } catch (IOException | InterruptedException e) {
            LOG.error("Could not delete metrics " + e);
        }

    }


}
