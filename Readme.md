You can run the uber jar for the multi threaded consumer like this 

java -classpath target/HelloKafka-1.0-SNAPSHOT-jar-with-dependencies.jar com.tilogaat.kafka.ConsumerGroupExample W.X.Y.Z tilo-group1 observations.json 3 true > filem2.txt

The arguments are as follows:
1.Zookeeper URL
2. consumer group id as a string
3. topic name
4. number of threads
5. true/false to turn logging of messages on


# Running Simple Consumer
java -classpath target/HelloKafka-1.0-SNAPSHOT-jar-with-dependencies.jar com.tilogaat.kafka.SimpleExample 1000000 observations.json 0 localhost 9092 > files2.txt

Arguments:
1. Total number of messages to be read
2. topic
3. partition id
4. seed broker 
5. port number 
