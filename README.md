# Threader
This frame work takes messages from an input topic. 
Adds the messages to a threadpool.  
The thread pool then executes the Worker callable for processing
The worker then returns the result of the processing for production to an OUTPUT topic.

You are responsible for dealing with exceptions in the worker/processor.

Idempotency takes care of the rest.

https://github.com/confluentinc/FRB_threader/blob/master/src/main/java/com/distributedlog/threadedWorkers.java#L41 <-- LOGIC GOES THERE!

java -jar target/threader-jar-with-dependencies.jar --producer-properties src/main/resources/producer.properties --consumer-properties src/main/resources/consumer.properties --thread-properties src/main/resources/thread.properties
    
