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
public class KafkaGroupProducerApp {
    public static void main(String[] args) {
        
        //Create a properties dictionary for the required/optional Producer config settings.
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        KafkaProducer<String, String> myProducer = new KafkaProducer<String, String>(props);
        
        try{
            int counter=0;
            while (counter < 100) {
                myProducer.send(new ProducerRecord<String, String>("MY_BIG_TOPIC", "abcdefghijklmnopqrstuvwxyz"));
                counter++;
            }           
        } catch(Exception e){
            e.printStackTrace();
        } finally {
            myProducer.close();
        }
        
    }
}
