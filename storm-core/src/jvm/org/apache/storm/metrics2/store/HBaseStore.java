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


import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
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
    private final static String HBASE_ZK_KEY = BASE_CONFIG_KEY + ".zookeeper";
    private final static String STORM_ZK_KEY = "storm.zookeeper";

    private final static String ZOOKEEPER_SERVERS = ".servers";
    private final static String ZOOKEEPER_PORT = ".port";
    private final static String ZOOKEEPER_ROOT = ".root";
    private final static String ZOOKEEPER_SESSION_TIMEOUT = ".session.timeout";

    private final static String RETENTION = BASE_CONFIG_KEY + ".retention";
    private final static String RETENTION_UNIT = BASE_CONFIG_KEY + ".retention.units";

    private final static String HBASE_ROOT_DIR = BASE_CONFIG_KEY + ".hbase.root_dir";
    private final static String HBASE_META_TABLE = BASE_CONFIG_KEY + ".hbase.metrics_table";
    private final static String HBASE_META_CF = BASE_CONFIG_KEY + ".hbase.meta_cf";
    private final static String HBASE_META_COL = BASE_CONFIG_KEY + ".hbase.meta_column";
    private final static String HBASE_METRICS_TABLE = BASE_CONFIG_KEY + ".hbase.metrics_table";
    private final static String HBASE_METRICS_CF = BASE_CONFIG_KEY + ".hbase.metrics_cf";
    private final static String HBASE_METRICS_VALUE_COL = BASE_CONFIG_KEY + ".hbase.metrics_value_column";
    private final static String HBASE_METRICS_COUNT_COL = BASE_CONFIG_KEY + ".hbase.metrics_count_column";
    private final static String HBASE_METRICS_SUM_COL = BASE_CONFIG_KEY + ".hbase.metrics_sum_column";
    private final static String HBASE_METRICS_MIN_COL = BASE_CONFIG_KEY + ".hbase.metrics_min_column";
    private final static String HBASE_METRICS_MAX_COL = BASE_CONFIG_KEY + ".hbase.metrics_max_column";

    private Connection _hbaseConnection;
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

        HBaseSchema schema = new HBaseSchema(config);
        Configuration hbaseConf = createHBaseConfiguration(config);

        try {
            this._hbaseConnection = ConnectionFactory.createConnection(hbaseConf);
            Admin hbaseAdmin = _hbaseConnection.getAdmin();
            TableName name = schema.metricsTableInfo.getTableName();
            HTableDescriptor descriptor = schema.metricsTableInfo.getDescriptor();

            if (!hbaseAdmin.tableExists(name)) {
                hbaseAdmin.createTable(descriptor);
            }

            this._metricsTable = _hbaseConnection.getTable(schema.metricsTableInfo.getTableName());
            this._serializer = new HBaseSerializer(_hbaseConnection, schema);
        } catch (IOException e) {
            throw new MetricException("Could not connect to hbase " + e);
        }
    }

    private void validateConfig(Map config) throws MetricException {
        // TODO: add rest of validation

        if (!config.containsKey(HBASE_ROOT_DIR)) {
            throw new MetricException("Need HBase root dir");
        }

        if (!config.containsKey(HBASE_METRICS_TABLE)) {
            throw new MetricException("Need metrics table");
        }

    }

    private Configuration createHBaseConfiguration(Map config) {
        // TODO: read from config, fix cast?
        Configuration conf = HBaseConfiguration.create();

        Object zookeeperServers = config.get(HBASE_ZK_KEY + ZOOKEEPER_SERVERS);
        String zkPrefix = (zookeeperServers == null) ? STORM_ZK_KEY : HBASE_ZK_KEY;

        String hbaseRootDir = (String) config.get(HBASE_ROOT_DIR);
        String zookeeperQuorum = String.join(";", (List) config.get(zkPrefix + ZOOKEEPER_SERVERS));
        String zookeeperRoot = (String) config.get(zkPrefix + ZOOKEEPER_ROOT);
        int zookeeperPort = (int) config.get(zkPrefix + ZOOKEEPER_PORT);
        int zookeeperSessionTimeout = (int) config.get(zkPrefix + ZOOKEEPER_SESSION_TIMEOUT);
        // TODO : username/password - null

        conf.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum);
        conf.set(HConstants.ZOOKEEPER_DATA_DIR, zookeeperRoot);
        conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zookeeperPort);
        conf.setInt(HConstants.ZK_SESSION_TIMEOUT, zookeeperSessionTimeout);

        // security
        //conf.set("hbase.security.authentication", "kerberos");
        //conf.set("hbase.rpc.protection", "privacy");

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

    // testing only
    public HBaseSerializer getSerializer() {
        return this._serializer;
    }

    public Table getMetricsTable() {
        return this._metricsTable;
    }

}
