package org.phy2000.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

class threadedWorkers {
    private static Properties producerConfigs;
    private static Properties consumerConfigs;
    private static Properties threadConfigs;
    // --Commented out by Inspection (12/23/19, 8:31 PM):private Properties threadConfigs;
    private static String INPUT_TOPIC;
    private static String OUTPUT_TOPIC;

    public void setProducerConfigs(Properties producerProperties ){
        producerConfigs = producerProperties;
    }
    public void setConsumerConfigs(Properties consumerConfigs){
        threadedWorkers.consumerConfigs = consumerConfigs;
    }
    public void setThreadConfigs(Properties threadConfigs) {
        this.threadConfigs = threadConfigs;
    }
    public void setInputTopic(String INPUT_TOPIC) {
        threadedWorkers.INPUT_TOPIC = INPUT_TOPIC;
    }
    public void setOutputTopic(String OUTPUT_TOPIC) {
        threadedWorkers.OUTPUT_TOPIC = OUTPUT_TOPIC;
    }


    public static class Worker implements Callable<ProducerRecord<String, String>> {
        final ConsumerRecord<String, String> record;

        public Worker(final ConsumerRecord<String, String> record) {
            this.record = record;
        }

        @Override
        public ProducerRecord<String, String> call() throws Exception {
            final long start = System.currentTimeMillis();
            System.out.println("started " + record.offset());
            Thread.sleep(200);
            final ProducerRecord<String, String> result = new ProducerRecord<>(
                    "outputtopic",
                    null,
                    record.key(),
                    "PROCESSED: " + record.value(),
                    record.headers()
            );
            result.headers().add("processStart", ByteBuffer.allocate(Long.BYTES).putLong(start).array());
            result.headers().add("processEnd", ByteBuffer.allocate(Long.BYTES).putLong(System.currentTimeMillis()).array());
            System.out.println("finished " + record.offset());
            return result;
        }
    }

    public void multiThreadedProcessor(final int numberOfThreads, final Function<ConsumerRecord<String, String>, Worker> workerFactory) throws InterruptedException, ExecutionException {
        final String groupId = new ConsumerConfig(consumerConfigs).getString(ConsumerConfig.GROUP_ID_CONFIG);
        final ExecutorService pool = Executors.newFixedThreadPool(numberOfThreads);
        final Map<TopicPartition, TreeMap<Long, Future<ProducerRecord<String, String>>>> inFlightPerPartition = new HashMap<>();


        try (final Consumer<String, String> consumer = new KafkaConsumer<>(consumerConfigs);
             final Producer<String, String> producer = new KafkaProducer<>(producerConfigs)) {
            consumer.subscribe(Collections.singleton("inputtopic"));
            //producer.initTransactions();

            //producer.beginTransaction();
            while (true) {
                final ConsumerRecords<String, String> poll = consumer.poll(Duration.ZERO);
                for (final ConsumerRecord<String, String> inputRecord : poll) {
                    final Future<ProducerRecord<String, String>> outputRecordFuture = pool.submit(workerFactory.apply(inputRecord));
                    final TopicPartition inputTopicPartition = new TopicPartition(inputRecord.topic(), inputRecord.partition());
                    inFlightPerPartition
                            .computeIfAbsent(inputTopicPartition, (ignore) -> new TreeMap<>())
                            .put(inputRecord.offset(), outputRecordFuture)
                    ;
                }

                LinkedList<Long> keyToRemove = new LinkedList<>();
                //final Map<TopicPartition, OffsetAndMetadata> offsetsToSend = new HashMap<>();
                for (final Map.Entry<TopicPartition, TreeMap<Long, Future<ProducerRecord<String, String>>>> inFlight : inFlightPerPartition.entrySet()) {
                    for (final Map.Entry<Long, Future<ProducerRecord<String, String>>> entry : inFlight.getValue().entrySet()) {

                        if (entry.getValue().isDone()) {
                            producer.send(entry.getValue().get());

                            //offsetsToSend.put(inFlight.getKey(), new OffsetAndMetadata(entry.getKey(), ""));
                            //keyToRemove.add(entry.getKey());
                        } else {
                            break;
                        }
                    }
                    for (Long aLong : keyToRemove) {
                        inFlight.getValue().remove(aLong);
                    }
                    keyToRemove.clear();
                }
/*
                if (!offsetsToSend.isEmpty()) {

                    try {
                        producer.
                        //producer.sendOffsetsToTransaction(offsetsToSend, groupId);
                    } catch( KafkaException e) {
                        e.printStackTrace();
                    }
                    //producer.commitTransaction();
                    //producer.flush();
                    //producer.beginTransaction();
                }*/
            }
        } catch ( RuntimeException e) {
            e.printStackTrace();
        }
    }
}
