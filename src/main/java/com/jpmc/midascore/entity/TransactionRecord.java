package com.jpmc.midascore.entity;

import com.jpmc.midascore.foundation.Transaction;

public class TransactionRecord {


    private Transaction transaction;
    private long senderId;
    private long recipientId;
    private float amount;

    public TransactionRecord(Transaction transaction){
    this.transaction = transaction;
    this.senderId = transaction.getSenderId();
    this.recipientId = transaction.getRecipientId();
    this.amount = transaction.getAmount();
    }

    public long getSenderId(){
    return senderId;
    }
    public long getRecipientId(){
    return recipientId;
    }
    public float getAmount(){
    return amount;
    }


}
