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


import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

public class HBaseSchema {

    private static final String SCHEMA_KEY = "storm.metrics2.store.HBaseStore.hbase.schema";

    public class MetricsTableInfo {

        private TableName tableName;
        private HTableDescriptor descriptor;
        private byte[] columnFamily;
        private byte[] valueColumn;
        private byte[] sumColumn;
        private byte[] countColumn;
        private byte[] minColumn;
        private byte[] maxColumn;

        public MetricsTableInfo(String tableName,
                                String columnFamily,
                                String valueColumn,
                                String sumColumn,
                                String countColumn,
                                String minColumn,
                                String maxColumn) {
            this.tableName = TableName.valueOf(tableName);
            this.columnFamily = Bytes.toBytes(columnFamily);
            this.valueColumn = Bytes.toBytes(valueColumn);
            this.sumColumn = Bytes.toBytes(sumColumn);
            this.countColumn = Bytes.toBytes(countColumn);
            this.minColumn = Bytes.toBytes(minColumn);
            this.maxColumn = Bytes.toBytes(maxColumn);

            HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);
            this.descriptor = new HTableDescriptor(this.tableName).addFamily(columnDescriptor);
        }

        public TableName getTableName() {
            return tableName;
        }

        public HTableDescriptor getDescriptor() {
            return descriptor;
        }

        public byte[] getColumnFamily() {
            return columnFamily;
        }

        public byte[] getValueColumn() {
            return valueColumn;
        }

        public byte[] getSumColumn() {
            return sumColumn;
        }

        public byte[] getCountColumn() {
            return countColumn;
        }

        public byte[] getMinColumn() {
            return minColumn;
        }

        public byte[] getMaxColumn() {
            return maxColumn;
        }
    }

    public class MetadataTableInfo {

        private TableName tableName;
        private HTableDescriptor descriptor;
        private byte[] columnFamily;
        private byte[] column;

        public MetadataTableInfo(String tableName, String columnFamily, String column) {
            this.tableName = TableName.valueOf(tableName);
            this.columnFamily = Bytes.toBytes(columnFamily);
            this.column = Bytes.toBytes(column);

            HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);
            this.descriptor = new HTableDescriptor(this.tableName).addFamily(columnDescriptor);
        }

        public TableName getTableName() {
            return tableName;
        }

        public HTableDescriptor getDescriptor() {
            return descriptor;
        }

        public byte[] getColumnFamily() {
            return columnFamily;
        }

        public byte[] getColumn() {
            return column;
        }
    }

    private final static int TOPOLOGY = 0;
    private final static int STREAM = 1;
    private final static int HOST = 2;
    private final static int COMP = 3;
    private final static int METRICNAME = 4;
    private final static int EXECUTOR = 5;

    public MetricsTableInfo metricsTableInfo;
    public MetadataTableInfo[] metadataTableInfos;

    public HBaseSchema(Map conf) {

        HashMap<String, Object> schemaMap = (HashMap<String, Object>) conf.get(SCHEMA_KEY);
        HashMap<String, Object> metricsMap = (HashMap<String, Object>) schemaMap.get("metrics");
        HashMap<String, Object> metadataMap = (HashMap<String, Object>) schemaMap.get("metadata");
        this.metadataTableInfos = new MetadataTableInfo[6];

        createMetricsDescriptor(metricsMap);

        metadataMap.forEach((metadataType, map) -> {
            HashMap<String, String> tableMap = (HashMap<String, String>) map;
            createMetadataDescriptor(metadataType, tableMap);
        });

    }

    private void createMetricsDescriptor(Map metricsMap) {

        HashMap<String, String> columnMap = (HashMap<String, String>) metricsMap.get("columns");

        String name = (String) metricsMap.get("name");
        String columnFamily = (String) metricsMap.get("cf");
        String valueColumn = columnMap.get("value");
        String sumColumn = columnMap.get("sum");
        String countColumn = columnMap.get("count");
        String minColumn = columnMap.get("min");
        String maxColumn = columnMap.get("max");

        this.metricsTableInfo = new MetricsTableInfo(name, columnFamily, valueColumn, sumColumn,
                countColumn, minColumn, maxColumn);

    }

    private void createMetadataDescriptor(String metadataType, HashMap<String, String> tableMap) {

        String name = tableMap.get("name");
        String columnFamily = tableMap.get("cf");
        String column = tableMap.get("column");

        MetadataTableInfo info = new MetadataTableInfo(name, columnFamily, column);

        switch (metadataType) {
            case "topoMap":
                this.metadataTableInfos[TOPOLOGY] = info;
                break;
            case "streamMap":
                this.metadataTableInfos[STREAM] = info;
                break;
            case "hostMap":
                this.metadataTableInfos[HOST] = info;
                break;
            case "compMap":
                this.metadataTableInfos[COMP] = info;
                break;
            case "metricMap":
                this.metadataTableInfos[METRICNAME] = info;
                break;
            case "executorMap":
                this.metadataTableInfos[EXECUTOR] = info;
                break;
        }

    }


}
