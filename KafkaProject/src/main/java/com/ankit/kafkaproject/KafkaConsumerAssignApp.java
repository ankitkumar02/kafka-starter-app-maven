/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ankit.kafkaproject;

import java.util.ArrayList;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 *
 * @author ankit
 */
public class KafkaConsumerAssignApp {
    public static void main(String[] args) {
        
        // Create a properties dictionary for the required/optional Producer Config settings.
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "test");
        
        
        KafkaConsumer myConsumer = new KafkaConsumer(props);
       
        ArrayList<TopicPartition> partitions = new ArrayList<TopicPartition>();
        TopicPartition myNewTopicPart0 = new TopicPartition("MY_NEW_TOPIC", 0);
        TopicPartition mySecondTopicPart2 = new TopicPartition("MY_SECOND_TOPIC", 2);
        
        partitions.add(myNewTopicPart0);
        partitions.add(mySecondTopicPart2);
        
        myConsumer.assign(partitions);
        
               
//        Less-than-intuitive unsubscribe alternatives:
//        topics.clear();  // Emptying out the list
//        myConsumer.subscribe(topics);

        try {
            while(true){
                ConsumerRecords<String, String> records = myConsumer.poll(10);
                for (ConsumerRecord<String, String> record : records){
                    //Process each record
                    System.out.println
                        (String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s",
                                record.topic(), record.partition(),record.offset(), record.key(), record.value()));
                }
            }
        } catch(Exception e){
            System.out.println("Error Occured In Consumer:"+ e.getMessage());
               
                
        } finally {
           myConsumer.close(); 
        }
    }
}
