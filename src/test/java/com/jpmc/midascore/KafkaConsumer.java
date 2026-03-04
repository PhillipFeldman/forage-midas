package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {


    @KafkaListener(topics = {"${general.kafka-topic}"},groupId = "com.jpmc")
    public void consume(Transaction quote){
        System.out.println("I Received " + quote);
    }



}
