package org.apache.storm.metrics2.store;

import java.util.HashMap;

class RocksDBBench {
    public static void main(String[] args){
        RocksDBConnector conn = new RocksDBConnector();

        HashMap<String, Object> conf = new HashMap<>();
        conf.put("storm.metrics2.store.rocksdb.create_if_missing", true);
        conf.put("storm.metrics2.store.rocksdb.location", "/tmp/rocks_bench");
        conn.prepare(conf);

        String topo = "topo1";
        int metricCount = 400;

        System.out.println ("Insert test");
        for (int c = 0; c < 100; c++){
            long startTime = System.currentTimeMillis();
            Double value = 0.0;
            for (int i = 0; i < metricCount; i++){
                String metric = "metric" + i + c;
                
                int samplesADay = 6*60*24;
                int samplesAMonth = samplesADay * 30;
                for (long j = 0; j < samplesADay; j++){
                    Metric m = new Metric(metric, j, "[1-1]", "comp1", topo, value);
                    conn.insert(m);
                    value++;
                }
            }

            long time = System.currentTimeMillis() - startTime;
            System.out.println(c + ": Wrote " + value + " rows in " + time + " ms");

            startTime = System.currentTimeMillis();
            System.out.println("Sequential scan test");
            long sum = conn.scanSum();
            time = System.currentTimeMillis() - startTime;
            System.out.println("SUM: " + sum + " in " + time + " ms");
        }
    }
}
