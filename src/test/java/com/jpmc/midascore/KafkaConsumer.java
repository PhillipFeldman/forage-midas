package com.jpmc.midascore;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    @Autowired
    private DatabaseConduit databaseConduit;


    @KafkaListener(topics = {"${general.kafka-topic}"}, groupId = "com.jpmc")
    public void consume(String quote) {
        System.out.println("I Received " + quote);

        String[] transactionData = quote.split(",");
        Transaction transaction = new Transaction(
                Long.parseLong(transactionData[0].split(":")[1]),
                Long.parseLong(transactionData[1].split(":")[1]),
                Float.parseFloat(transactionData[2].split(":")[1].replace("}", ""))
        );
        System.out.println(transaction);
        TransactionRecord transactionRecord = new TransactionRecord(transaction);
        //verifyTransaction(transactionRecord);
        UserRecord record = databaseConduit.findById(transactionRecord.getSenderId());
        System.out.println("This is the record: "+record);

    }

    @Autowired
    private TransactionRecordRepository transactionRecordRepository;

    private boolean verifyTransaction(TransactionRecord transactionRecord) {
        transactionRecordRepository.findBySenderID(transactionRecord.getSenderId());


        return false;

    }


}
