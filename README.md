# parallel-consumer-example
Code repo showing a comparision of simple consumer and parallel consumer

Clone the repository 
```sh
git clone https://github.com/lokeshallam/parallel-consumer-example
```

Build the project
```sh
mvn clean compile package
```

Shaded jar will be generated under target/examples-1.0.0.jar

Review the consumer properties src/main/resources/simpleconsumer.properties and /Users/lallam/src/main/resources/parallelconsumer.properties 
update the `bootstrap.servers` and add security related configuration as most of the kafka deployments will have authentication enabled. 

General cloud connection configurations will be below 

```sh
security.protocol=SASL_SSL
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='REplaceApi-KEY' password='ReplaceAPI-Secret';
sasl.mechanism=PLAIN
# Required for correctness in Apache Kafka clients prior to 2.6
client.dns.lookup=use_all_dns_ips
```


To run a simple Consumer 

```sh
java -cp target/examples-1.0.0.jar com.lokesh.demo.SimpleConsumer /Users/lallam/Downloads/parallel-consumer-example/src/main/resources/simpleconsumer.properties
```

To run a parallel Consumer

```sh
java -cp target/examples-1.0.0.jar com.lokesh.demo.ParallelConsumer /Users/lallam/Downloads/parallel-consumer-example/src/main/resources/parallelconsumer.properties
```

Parallel Consumer shown here is very basic version. More details can be found at https://github.com/confluentinc/parallel-consumer