package kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;


public class KafkaConsumer {
	
	private ConsumerConnector consumerConnector = null;
    private final String topic = "campaignData_kafka_queue";

    public void initialize() {
          Properties props = new Properties();
          props.put("zookeeper.connect", "localhost:2181");
          props.put("group.id", "testgroup");
          props.put("zookeeper.session.timeout.ms", "400");
          props.put("zookeeper.sync.time.ms", "300");
          props.put("auto.commit.interval.ms", "1000");
          ConsumerConfig conConfig = new ConsumerConfig(props);
          consumerConnector = Consumer.createJavaConsumerConnector(conConfig);
    }

    public void consume() {
          //Key = topic name, Value = No. of threads for topic
          Map<String, Integer> topicCount = new HashMap<String, Integer>();       
          topicCount.put(topic, new Integer(1));
         
          //ConsumerConnector creates the message stream for each topic
          Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams =
                consumerConnector.createMessageStreams(topicCount);         
         
          // Get Kafka stream for topic 
          List<KafkaStream<byte[], byte[]>> kStreamList =
                                               consumerStreams.get(topic);
          
          String message;
          // Iterate stream using ConsumerIterator
          for (final KafkaStream<byte[], byte[]> kStreams : kStreamList) {
                 ConsumerIterator<byte[], byte[]> consumerIte = kStreams.iterator();
                
                 while (consumerIte.hasNext()){
                	 message=new String(consumerIte.next().message());
                        System.out.println("Message consumed from topic[" + topic + "] : "+message);  
                         MessageParser.parseMessage(message);
                
                 }
          }
          //Shutdown the consumer connector
          if (consumerConnector != null)   consumerConnector.shutdown();          
    }

    public static void main(String[] args) throws InterruptedException {
          KafkaConsumer kafkaConsumer = new KafkaConsumer();
          // Configure Kafka consumer
          kafkaConsumer.initialize();
          // Start consumption
          kafkaConsumer.consume();
          
         // BasicConfigurator.configure();
    }

}