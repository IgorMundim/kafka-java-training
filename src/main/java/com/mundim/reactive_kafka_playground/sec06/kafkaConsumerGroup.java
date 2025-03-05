package com.mundim.reactive_kafka_playground.sec06;

import com.mundim.reactive_kafka_playground.sec05.KafkaConsumer;

public class kafkaConsumerGroup {
    private static class Consumer1 {
        public static void main(String[] args) {
            com.mundim.reactive_kafka_playground.sec05.KafkaConsumer.start("1");
        }
    }
    private static class Consumer2 {
        public static void main(String[] args) {
            com.mundim.reactive_kafka_playground.sec05.KafkaConsumer.start("2");
        }
    }
    private static class Consumer3 {
        public static void main(String[] args) {
            KafkaConsumer.start("3 ");
        }
    }

}
