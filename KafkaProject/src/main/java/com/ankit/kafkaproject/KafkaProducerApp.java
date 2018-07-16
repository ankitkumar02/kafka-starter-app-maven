/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ankit.kafkaproject;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 *
 * @author ankit
 */
public class KafkaProducerApp {
    public static void main(String[] args) {
        
        //Create a properties dictionary for the required/optional Producer config settings.
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        KafkaProducer<String, String> myProducer = new KafkaProducer<String, String>(props);
        
        try{
            for(int i=0; i<150; i++){
                // ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value);
                myProducer.send(new ProducerRecord<String, String>("REST_PROC_REQ_1", Integer.toString(i),"MyMessage :"+ Integer.toString(i)));
            }            
        } catch(Exception e){
            e.printStackTrace();
        } finally {
            myProducer.close();
        }
        
    }
}
