package com.wxmimperio.flink;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by wxmimperio on 2017/7/21.
 */
public class FlinkKafkaConsumer {

    public static void main(String[] args) throws Exception {
        // create execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties propertiesProducer = new Properties();
        propertiesProducer.setProperty("bootstrap.servers", "192.168.1.112:9092");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.112:9092");
        properties.setProperty("group.id", "flink_consumer");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer010<String>("flink_demo3", new SimpleStringSchema(), properties));

        stream.map(new MapFunction<String, String>() {
            private static final long serialVersionUID = -6867736771747690202L;

            @Override
            public String map(String value) throws Exception {
                /*String[] message = value.split("\\^_\\^");
                Map<String, String> map = new HashMap<>();
                for (String str : message) {
                    if (!str.isEmpty()) {
                        String[] tmp = str.split(":");
                        map.put(tmp[0], tmp[1]);
                    }
                }
                return map.toString();*/
                return value;
            }
        })
        //.addSink(new FlinkKafkaProducer010<String>("flink_demo3", new SimpleStringSchema(), properties));
        .print();

        env.execute();
    }
}
