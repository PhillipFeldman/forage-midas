package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {



    @KafkaListener(topics = {"${general.kafka-topic}"}, groupId = "com.jpmc")
    public void consume(String quote) {
        System.out.println("I Received " + quote);

        String[] transactionData = quote.split(",");
        Transaction transaction = new Transaction(
                Long.parseLong(transactionData[0].split(":")[1]),
                Long.parseLong(transactionData[1].split(":")[1]),
                Float.parseFloat(transactionData[2].split(":")[1].replace("}", ""))
        );
        System.out.println("transaction: "+transaction);


    }



}
