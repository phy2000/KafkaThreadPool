package com.distributedlog;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.function.Function;

class threader {
    public static void main (String[] args) throws java.io.IOException {
        Options options = new Options();
        CommandLineParser parser = new DefaultParser();
        CommandLine line = null;

        try {
            line = parser.parse( makeOptions(), args );

        }
        catch( ParseException exp ) {
            System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
        }

        HashMap<String, String> consumerProperties = parseFile(line.getOptionValue("consumer-properties"));
        HashMap<String, String> producerProperties = parseFile(line.getOptionValue("producer-properties"));
        HashMap<String, String> threadProperties = parseFile(line.getOptionValue("thread-properties"));

        // ADD STUFF FROM JOHN FOR LOW LATENCY
        //setup consumer
        final Properties consumerConfigs = new Properties();
        consumerConfigs.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerProperties.get("bootstrap.servers"));
        consumerConfigs.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerProperties.get("group.id"));
        consumerConfigs.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfigs.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfigs.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //consumerConfigs.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerConfigs.setProperty(ConsumerConfig.RECEIVE_BUFFER_CONFIG, consumerProperties.get("receive.buffer.bytes"));
        consumerConfigs.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, consumerProperties.get("fetch.min.bytes"));
        consumerConfigs.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, consumerProperties.get("fetch.max.bytes"));
        consumerConfigs.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, consumerProperties.get("max.partition.fetch.bytes"));
        consumerConfigs.setProperty(ConsumerConfig.SEND_BUFFER_CONFIG, consumerProperties.get("send.buffer.bytes"));
        consumerConfigs.setProperty(ConsumerConfig.CLIENT_RACK_CONFIG, consumerProperties.get("client.rack"));
        consumerConfigs.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, consumerProperties.get("fetch.max.wait.ms"));

        //setup producer
        final Properties producerConfigs = new Properties();
        producerConfigs.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerProperties.get("bootstrap.servers"));
        //producerConfigs.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, producerProperties.get("transactional.id"));
        producerConfigs.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfigs.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
       // producerConfigs.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerConfigs.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, producerProperties.get("buffer.memory"));
        producerConfigs.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, producerProperties.get("compression.type"));
        producerConfigs.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, producerProperties.get("batch.size"));
        producerConfigs.setProperty(ProducerConfig.LINGER_MS_CONFIG, producerProperties.get("linger.ms"));
        producerConfigs.setProperty(ProducerConfig.RECEIVE_BUFFER_CONFIG, producerProperties.get("receive.buffer.bytes"));


        //thread options
        final Properties threadConfigs = new Properties();
        threadConfigs.setProperty("num.threads", threadProperties.get("num.threads"));
        threadConfigs.setProperty("sleep.time", threadProperties.get("sleep.time"));
        threadConfigs.setProperty("input.topic", threadProperties.get("input.topic"));
        threadConfigs.setProperty("output.topic", threadProperties.get("output.topic"));

        threadedWorkers tw = new threadedWorkers();
        tw.setProducerConfigs(producerConfigs);
        tw.setConsumerConfigs(consumerConfigs);
        tw.setThreadConfigs(threadConfigs);
        
        tw.setInputTopic(consumerConfigs.getProperty("input.topic"));
        tw.setOutputTopic(producerConfigs.getProperty("output.topic"));

        final Function<ConsumerRecord<String, String>, threadedWorkers.Worker> workerFactory = threadedWorkers.Worker::new;


        try {
            tw.multiThreadedProcessor(Integer.parseInt(threadConfigs.getProperty("num.threads")), workerFactory);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    private static Options makeOptions() {
        Option cp = Option.builder("c").longOpt("consumer-properties")
                .required()
                .hasArg()
                .desc("consumer properties file")
                .build();
        Option pp = Option.builder("p").longOpt("producer-properties").required().hasArg().desc("Producer properties file").build();
        Option tp = Option.builder("t").longOpt("thread-properties").required().hasArg().desc("thread options file").build();
        final Options options = new Options();

        options.addOption(pp);
        options.addOption(cp);
        options.addOption(tp);

        return options;

    }

     private static HashMap<String, String> parseFile(String propFile) throws IOException {


        HashMap<String, String> map = new HashMap<>();
        BufferedReader buff = new BufferedReader(new FileReader(new File(propFile)));

        String lineOfFile;
        while((lineOfFile = buff.readLine()) != null) {
            if((!lineOfFile.startsWith("#")) && !lineOfFile.isEmpty()) {
                String[] pair = lineOfFile.trim().split("=");
                map.put(pair[0].trim(), pair[1].trim());
            }
        }
        buff.close();
        return map;
    }
}
