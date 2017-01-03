/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.metrics2.store;

import java.lang.String;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.TableFormatConfig;
import org.rocksdb.IndexType;
import org.rocksdb.CompactionStyle;

/**
 * This class implements the Storm Metrics Store Interface using RocksDB as a store
 * It contains preparing, insertion and scan methods to store and query metrics
 *
 * @author Austin Chung <achung13@illinois.edu>
 * @author Abhishek Deep Nigam <adn5327@gmail.com>
 * @author Naren Dasan <naren@narendasan.com>
 */

public class RocksDBConnector implements MetricStore {
    private final static Logger LOG = LoggerFactory.getLogger(RocksDBConnector.class);
    private RocksDB db;

    /**
     * Implements the prepare method of the Metric Store, create RocksDB instance
     * using the configurations provided via the config map
     * @param config Storm config map
     */
    @Override
    public void prepare(Map config) {

        try {
            validateConfig(config);
        } catch(MetricException e) {
            LOG.error("Invalid config for RocksDB metrics store", e);
            //TODO-AB: throw a runtime error
        }

        RocksDB.loadLibrary();
        // the Options class contains a set of configurable DB options
        // that determines the behavior of a database.
        //Utils.getString
        boolean createIfMissing = Boolean.parseBoolean(config.get("storm.metrics2.store.rocksdb.create_if_missing").toString());
        Options options = new Options().setCreateIfMissing(createIfMissing);

        if (config.containsKey("storm.metrics2.store.rocksdb.total_threads")) {
            options.setIncreaseParallelism((int)config.get("storm.metrics2.store.rocksdb.total_threads"));
        }

        if (config.containsKey("storm.metrics2.store.rocksdb.optimize_filters_for_hits")) {
            options.setOptimizeFiltersForHits((boolean)config.get("storm.metrics2.store.rocksdb.optimize_filters_for_hits"));
        }

        if (config.containsKey("storm.metrics2.store.rocksdb.optimize_level_style_compaction")) {
            if ((boolean)config.get("storm.metrics2.store.rocksdb.optimize_level_style_compaction")) {
                if (config.containsKey("storm.metrics2.store.rocksdb.optimize_level_style_compaction_memtable_memory_budget_mb")) {
                    Integer budget_mb = (Integer)config.get("storm.metrics2.store.rocksdb.optimize_level_style_compaction_memtable_memory_budget_mb");
                    options.optimizeLevelStyleCompaction(budget_mb * 1024L * 1024L);
                } else {
                    options.optimizeLevelStyleCompaction();
                }
            }
        }

        // table format config
        String tableType = "plain";
        if (config.containsKey("storm.metrics2.store.rocksdb.table_type")){
            tableType = (String)config.get("storm.metrics2.store.rocksdb.table_type");
        }

        if (true || tableType.equals("block")) {
            LOG.info("Instantiating RocksDB BlockBasedTable");
            BlockBasedTableConfig tfc = new BlockBasedTableConfig();
            //tfc.setBlockCacheSize((long)16*1024*1024);
            tfc.setIndexType(IndexType.kHashSearch);
            tfc.setBlockSize((long)4*1024);
            options.useCappedPrefixExtractor(34); // epoch in ms length
            options.setMemtablePrefixBloomSizeRatio(10);
            options.setBloomLocality(1);
            options.setMaxOpenFiles(-1);
            options.setTableFormatConfig(tfc);
            options.setWriteBufferSize(32 << 20);
            options.setMaxWriteBufferNumber(2);
            options.setMinWriteBufferNumberToMerge(1);
            options.setVerifyChecksumsInCompaction(false);
            options.setDisableDataSync(true);
            options.setBytesPerSync(2 << 20);

            options.setCompactionStyle(CompactionStyle.UNIVERSAL);
            options.setLevelZeroFileNumCompactionTrigger(1);
            options.setLevelZeroSlowdownWritesTrigger(8);
            options.setLevelZeroStopWritesTrigger(16);
        } else {
            LOG.info("Instantiating RocksDB PlainTable");
            PlainTableConfig tfc = new PlainTableConfig();
            tfc.setFullScanMode(true);
            tfc.setStoreIndexInFile(true);
            options.setTableFormatConfig(tfc);
        }



        this.db = null;
        try {
            // a factory method that returns a RocksDB instance
            String path = config.get("storm.metrics2.store.rocksdb.location").toString();
            this.db = RocksDB.open(options, path);
            // do something
        } catch (RocksDBException e) {
            LOG.error("Error opening RockDB database", e);
        }

        LOG.info ("RocksDB Stats: {}", getStats());
    }

    public String getStats() {
        String stats = null;
        try {
            stats = this.db.getProperty("rocksdb.stats");
        } catch (RocksDBException e){
            LOG.error("Error getting RockDB database stats", e);
        }
        return stats;
    }

    /**
     * Implements the insert method of the Metric Store, stores metrics in the store
     * @param m Metric to store
     */
    @Override
    public void insert(Metric m) {
        try {
            LOG.debug("inserting {}", m.toString());
            byte[] key = m.getKeyBytes();
            byte[] value = m.getValueBytes();

            this.db.put(key, value);
        } catch (RocksDBException e) {
            LOG.error("Error inserting into RocksDB", e);
        }
    }

    /**
     * Implements scan method of the Metrics Store, scans all metrics in the store
     * @return List<String> metrics in store
     */

    @Override
    public void scan(IAggregator agg) {
        long test = 0L;
        RocksIterator iterator = this.db.newIterator();
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            Metric metric = new Metric(new String(iterator.key()));
            metric.setValueFromBytes(iterator.value());
            agg.agg(metric, null);
        }
    }

    /**
     * Implements scan method of the Metrics Store, scans all metrics with settings in the store
     * Will try to search the fastest way possible
     * @param settings map of settings to search by
     * @return List<String> metrics in store
     */
    @Override
    public void scan(HashMap<String, Object> settings, IAggregator agg){
        //IF CAN CREATE PREFIX -- USE THAT
        //ELSE DO FULL TABLE SCAN
        byte[] prefix = Metric.createPrefix(settings);
        if (prefix != null) {
            scan(prefix, settings, agg);
            return;
        }
        LOG.info("Cannot obtain prefix, doing full RocksDB scan");
        RocksIterator iterator = this.db.newIterator();

        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            String key = new String(iterator.key());
            LOG.info("At key: {}", key);

            Metric metric = new Metric(key);
            Set<TimeRange> timeRanges = checkMetric(metric, settings);
            if (timeRanges != null) {
                metric.setValueFromBytes(iterator.value());
                agg.agg(metric, timeRanges);
            }
        }
    }

    @Override
    public void remove(HashMap<String, Object> settings) {
        // for each key we match, remove it
        scan(settings, (metric, timeRanges) -> {
            try {
                this.db.remove(metric.getKey().getBytes());
            } catch (RocksDBException e) {
                LOG.error("Error removing from RocksDB", e);
            }
        });
    }

    @Override
    public boolean populateValue(Metric metric) {
        byte[] key = metric.getKeyBytes();
        try {
            byte[] value = this.db.get(key);
            metric.setValueFromBytes(value);
            return value != null ? true : false;
        } catch (RocksDBException ex){
            LOG.error("Exception getting value:", ex);
            return false;
        }
    }

    /**
     * Implements scan method of the Metrics Store, scans all metrics with prefix in the store
     * @param prefix prefix to query in store
     * @param settings search settings
     * @return List<String> metrics in store
     */
    private void scan(byte[] prefix, HashMap<String, Object> settings, IAggregator agg) {
        LOG.info("Prefix scan with {}", new String(prefix));
        RocksIterator iterator = this.db.newIterator();
        LOG.info("before");
        for (iterator.seek(prefix); iterator.isValid(); iterator.next()) {
            String key = new String(iterator.key());
            LOG.info("At key: {}", key);
            Metric metric = new Metric(key);

            Set<TimeRange> timeRanges = checkMetric(metric, settings);
            if (timeRanges != null){ 
                metric.setValueFromBytes(iterator.value());
                agg.agg(metric, timeRanges);
            } else {
                // skip, we may match something sliced inside of prefix
                continue;
            }
        }
        LOG.info("after {}", iterator.isValid());
    }

    /**
     * Implements configuration validation of Metrics Store, validates storm configuration for Metrics Store
     * @param config Storm config to specify which store type, location of store and creation policy
     * @throws MetricException if there is a missing required configuration or if the store does not exist but
     * the config specifies not to create the store
     */
    private void validateConfig(Map config) throws MetricException {
        if (!(config.containsKey("storm.metrics2.store.rocksdb.location"))) {
            throw new MetricException("Not a vaild RocksDB configuration - Missing store location");
        }

        if (!(config.containsKey("storm.metrics2.store.rocksdb.create_if_missing"))) {
            throw new MetricException("Not a vaild RocksDB configuration - Does not specify creation policy");
        }

        String createIfMissing = config.get("storm.metrics2.store.rocksdb.create_if_missing").toString();
        if (!Boolean.parseBoolean(createIfMissing)) {
            String storePath = config.get("storm.metrics2.store.rocksdb.location").toString();
            if (!(new File(storePath).exists())) {
                throw new MetricException("Configuration specifies not to create a store but no store currently exists");
            }
        }
        return;
    }

    /**
     * Implements configuration validation of Metrics Store, validates storm configuration for Metrics Store
     * @param possibleKey key to check
     * @param settings search settings
     * @throws MetricException if there is a missing required configuration or if the store does not exist but
     * the config specifies not to create the store
     */
    private Set<TimeRange> checkMetric(Metric possibleKey, HashMap<String, Object> settings)  {
        if(settings.containsKey(StringKeywords.aggLevel) && 
                (possibleKey.getAggLevel() == null ||
                 !possibleKey.getAggLevel().equals(settings.get(StringKeywords.aggLevel)))) {
            return null;
        } else if(settings.containsKey(StringKeywords.component) && 
                !possibleKey.getCompId().equals(settings.get(StringKeywords.component))) {
            return null;
        } else if(settings.containsKey(StringKeywords.metricName) && 
                !possibleKey.getMetricName().equals(settings.get(StringKeywords.metricName))) {
            return null;
        } else if(settings.containsKey(StringKeywords.topoId) && 
                !possibleKey.getTopoId().equals(settings.get(StringKeywords.topoId))) {
            return null;
        } else if(settings.containsKey(StringKeywords.timeRangeSet)){ 
            Set<TimeRange> timeRangeSet = (Set<TimeRange>)settings.get(StringKeywords.timeRangeSet);
            Set<TimeRange> matchedTimeRanges = new HashSet<TimeRange>();
            for (TimeRange tr : timeRangeSet){
                Long tstamp = possibleKey.getTimeStamp();
                if (tr.contains(tstamp)){
                    matchedTimeRanges.add(tr);
                }
            }
            return matchedTimeRanges.size() > 0 ? matchedTimeRanges : null;
        }
        return new HashSet<TimeRange>();
    }

    public void remove() {

    }

}
