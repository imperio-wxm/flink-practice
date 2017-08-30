package com.wxmimperio.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

/**
 * Created by wxmimperio on 2017/7/21.
 */
public class FlinkKafkaProducer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.112:9092");

        DataStream<String> stream = env.addSource(new SimpleStringGenerator());
        stream.addSink(new FlinkKafkaProducer010<String>("flink_demo1", new SimpleStringSchema(), properties));

        env.execute();
    }

    public static class SimpleStringGenerator implements SourceFunction<String> {
        private static final long serialVersionUID = 119007289730474249L;
        boolean running = true;
        long i = 0;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (running) {
                i++;
                String message = "^_^name:wxm" + (i) + "^_^age:" + (i);
                ctx.collect(message);
                System.out.println(message);

                Thread.sleep(2000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
