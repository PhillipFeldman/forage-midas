package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {



        @KafkaListener(topics = {"${general.kafka-topic}"},groupId = "com.jpmc")
        public void consume(String quote){
            System.out.println("I Received " + quote);
            //There's a bug somewhere below:
            /*
            String[] transactionData = quote.split(", ");
            Transaction transaction = new Transaction(
                            Long.parseLong(transactionData[0]),
                            Long.parseLong(transactionData[1]),
                            Float.parseFloat(transactionData[2])
                    );

*/
        }





}
