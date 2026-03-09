package com.jpmc.midascore.component;


import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import org.springframework.stereotype.Component;

@Component

public class TransactionDBConduit {
    private final TransactionRecordRepository transactionRecordRepository;

    public TransactionDBConduit(TransactionRecordRepository transactionRecordRepository){
    this.transactionRecordRepository = transactionRecordRepository;
    }

    public void save(TransactionRecord transactionRecord) {
        transactionRecordRepository.save(transactionRecord);
    }

}
