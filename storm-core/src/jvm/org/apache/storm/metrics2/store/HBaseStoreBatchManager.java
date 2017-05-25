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
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HConstants;


public class HBaseStoreBatchManager {
    // TODO: handle increment, append
    private final static Logger LOG = LoggerFactory.getLogger(HBaseStoreBatchManager.class);

    private List<Row> operationList;
    private HTable table;
    private byte[] defaultColumnFamily;
    private byte[] defaultColumn;


    public HBaseStoreBatchManager(HTable table) {
        this.operationList = new ArrayList<Row>();
        this.table = table;
    }

    public void setTable(HTable table) {
        this.table = table;
    }

    public HTable getTable() {
        return table;
    }

    public void setDefaultColumnFamily(byte[] columnFamily) {
        this.defaultColumnFamily = columnFamily;
    }

    public void setDefaultColumn(byte[] column) {
        this.defaultColumn = column;
    }

    public void put(byte key[], byte[] value) {
        put(key, value, defaultColumnFamily, defaultColumn);
    }

    public void put(byte[] key, byte[] value, byte[] columnFamily, byte[] column) {
        Put op = new Put(key);
        op.addColumn(columnFamily, column, value);
        operationList.add(op);
    }

    public void get(byte[] key) {
        Get op = new Get(key);
        operationList.add(op);
    }

    public void delete(byte[] key) {
        Delete op = new Delete(key);
        operationList.add(op);
    }

    public void execute(boolean atomic) throws IOException, InterruptedException {

        int opCount = operationList.size();

        if (opCount == 0)
            return;

        Object[] results = new Object[opCount];

        try {
            table.batch(operationList, results);
        } catch (IOException | InterruptedException e) {

            if (atomic && !checkResults(results)) {
                revertOperations(results);
            }
            throw e;
        }
    }

    private void revertOperations(Object[] results) throws IOException {
        for (int i = 0; i < results.length; ++i) {
            if (results[i] != null) {
                revert(operationList.get(i));
            }
        }
    }

    private void revert(Row op) throws IOException {
        // TODO: handle increment, append
        if (op instanceof Put) {
            Put p = (Put) op;
            Delete d = new Delete(p.getRow());
            table.delete(d);
        }
    }

    private boolean checkResults(Object[] results) {
        for (Object result : results) {
            if (result == null) return false;
        }
        return true;
    }
}
