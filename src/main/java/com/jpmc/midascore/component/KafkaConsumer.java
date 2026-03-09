package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

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
        System.out.println("transaction: " + transaction);

        UserRecord ur = getUserRecordById(transaction.getSenderId());
        System.out.println("sender" + ur);


    }


    private final DatabaseConduit service;

    public KafkaConsumer(DatabaseConduit service) {
        this.service = service;
    }

    @GetMapping("/{id}")
    public UserRecord getUserRecordById(@PathVariable Long id) {
        return service.getUserById(id);
    }


}
