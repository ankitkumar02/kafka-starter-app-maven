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

/**
 *
 * @author ankit
 */
public class KafkaConsumerGroupApp01 {
    public static void main(String[] args) {
        
        // Create a properties dictionary for the required/optional Producer Config settings.
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "test-group");
        
        
        KafkaConsumer myConsumer = new KafkaConsumer(props);
       
        ArrayList<String> topics = new ArrayList<String>();
        topics.add("MY_BIG_TOPIC");
        
        myConsumer.subscribe(topics);
        
               
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
                                record.topic(), record.partition(),record.offset(), record.key(), record.value().toUpperCase()));
                }
            }
        } catch(Exception e){
            System.out.println("Error Occured In Consumer:"+ e.getMessage());
               
                
        } finally {
           myConsumer.close(); 
        }
    }
}
