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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

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
    private HTable _metricsTable = null;
    private HBaseSerializer _serializer = null;

    /**
     * Create HBase instance
     * using the configurations provided via the config map
     *
     * @param config Storm config map
     */
    @Override
    public void prepare(Map config) throws MetricException {


        validateConfig(config);
        // setup
        initializeSchema(config);
        Configuration hbaseConf = createHBaseConfiguration(config);
        HTableDescriptor tableDesc = createMetricsTableDescriptor(config);
        // TODO: pass values
        this._serializer = new HBaseSerializer();

        // connect
        try {
            this._hbaseConnection = ConnectionFactory.createConnection(hbaseConf);
            this._hbaseAdmin = _hbaseConnection.getAdmin();
            if (!_hbaseAdmin.tableExists(tableDesc.getTableName())) {
                _hbaseAdmin.createTable(tableDesc);
            }

            // TODO: fix init, deprecated
            this._metricsTable = new HTable(tableDesc.getTableName(), _hbaseConnection);

        } catch (IOException e) {
            LOG.error("HBase Metrics initialization error ", e);
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

    private HTableDescriptor createMetricsTableDescriptor(Map config) {

        String name = (String) config.get(HBASE_METRICS_TABLE_KEY);
        TableName tableName = TableName.valueOf(name);
        HColumnDescriptor columnFamily = new HColumnDescriptor(COLUMN_FAMILY);

        HTableDescriptor descriptor = new HTableDescriptor(tableName);
        descriptor.addFamily(columnFamily);

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
            throw new MetricException("Need hbase root dir");
        }
        if (!(config.containsKey(HBASE_METRICS_TABLE_KEY))) {
            throw new MetricException("Need hbase metrics table");
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

        byte[] key = _serializer.serializeKey(m);
        long count = m.getCount();

        Put newEntry = new Put(key);

        byte[] mValue = Bytes.toBytes(m.getValue());
        newEntry.addColumn(COLUMN_FAMILY, COLUMN_VALUE, mValue);

        if (count > 1) {
            byte[] mCount = Bytes.toBytes(count);
            newEntry.addColumn(COLUMN_FAMILY, COLUMN_COUNT, mCount);

            byte[] mSum = Bytes.toBytes(m.getSum());
            newEntry.addColumn(COLUMN_FAMILY, COLUMN_SUM, mSum);

            byte[] mMin = Bytes.toBytes(m.getMin());
            newEntry.addColumn(COLUMN_FAMILY, COLUMN_MIN, mMin);

            byte[] mMax = Bytes.toBytes(m.getMax());
            newEntry.addColumn(COLUMN_FAMILY, COLUMN_MAX, mMax);
        }

        try {
            _metricsTable.put(newEntry);
        } catch (IOException e) {
            LOG.error("Could not insert metric", e, "// m =", m.toString());
        }

    }

    /**
     * Scans all metrics in the store
     *
     * @return void
     */
    @Override
    public void scan(IAggregator agg) {
        System.out.println("Not implemented - scan");
    }


    /**
     * Implements scan method of the Metrics Store, scans all metrics with settings in the store
     * Will try to search the fastest way possible
     *
     * @param settings map of settings to search by
     * @return List<Double> metrics in store
     */
    @Override
    public void scan(HashMap<String, Object> settings, IAggregator agg) {
        System.out.println("Not implemented - scan2");
    }

    // get by matching a key exactly
    @Override
    public boolean populateValue(Metric metric) {
        byte[] key = _serializer.serializeKey(metric);

        Get g = new Get(key);
        try {
            Result entry = _metricsTable.get(g);

            if (!entry.getExists())
                return false;

            byte[] value = entry.getValue(COLUMN_FAMILY, COLUMN_VALUE);
            if (value != null)
                metric.setValue(Bytes.toDouble(value));

            byte[] count = entry.getValue(COLUMN_FAMILY, COLUMN_COUNT);
            if (count != null)
                metric.setCount(Bytes.toLong(count));
            else
                metric.setCount(1);

            byte[] sum = entry.getValue(COLUMN_FAMILY, COLUMN_SUM);
            if (sum != null)
                metric.setValue(Bytes.toDouble(sum));

            byte[] min = entry.getValue(COLUMN_FAMILY, COLUMN_MIN);
            if (min != null)
                metric.setValue(Bytes.toDouble(min));

            byte[] max = entry.getValue(COLUMN_FAMILY, COLUMN_MAX);
            if (max != null)
                metric.setValue(Bytes.toDouble(max));

            return true;

        } catch (IOException e) {
            LOG.error("Could not retrieve metric", e);
            return false;
        }
    }

    // remove things matching settings, kind of like a scan but much scarier
    @Override
    public void remove(HashMap<String, Object> settings) {
        System.out.println("Not implemented - remove");
    }

}
