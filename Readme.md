You can run the uber jar for the multi threaded consumer like this 

java -classpath target/HelloKafka-1.0-SNAPSHOT-jar-with-dependencies.jar com.tilogaat.kafka.ConsumerGroupExample W.X.Y.Z tilo-group1 observations.json 3 true > filem2.txt

The arguments are as follows:
1.Zookeeper URL
2. consumer group id as a string
3. topic name
4. number of threads
5. true/false to turn logging of messages on
