package com.lyc.entity;

import org.apache.commons.lang.RandomStringUtils;

import java.util.UUID;

/**
 * @ProjectName bankrecord_bulkload
 * @ClassName TransferRecord
 * @Description TODO 实体类
 * @Author lyc
 * @Date 2022/5/19 14:58
 * @Version 1.0
 **/
public class TransferRecord {
    private String id;
    private String code;
    private String rec_account;
    private String rec_bank_name;
    private String rec_name;
    private String pay_account;
    private String pay_name;
    private String pay_comments;
    private String pay_channel;
    private String pay_way;
    private String status;
    private String timestamp;
    private Double money;

    public TransferRecord() {
        this.id = UUID.randomUUID().toString();
        this.code = RandomStringUtils.randomAlphanumeric(2);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getRec_account() {
        return rec_account;
    }

    public void setRec_account(String rec_account) {
        this.rec_account = rec_account;
    }

    public String getRec_bank_name() {
        return rec_bank_name;
    }

    public void setRec_bank_name(String rec_bank_name) {
        this.rec_bank_name = rec_bank_name;
    }

    public String getRec_name() {
        return rec_name;
    }

    public void setRec_name(String rec_name) {
        this.rec_name = rec_name;
    }

    public String getPay_account() {
        return pay_account;
    }

    public void setPay_account(String pay_account) {
        this.pay_account = pay_account;
    }

    public String getPay_name() {
        return pay_name;
    }

    public void setPay_name(String pay_name) {
        this.pay_name = pay_name;
    }

    public String getPay_comments() {
        return pay_comments;
    }

    public void setPay_comments(String pay_comments) {
        this.pay_comments = pay_comments;
    }

    public String getPay_channel() {
        return pay_channel;
    }

    public void setPay_channel(String pay_channel) {
        this.pay_channel = pay_channel;
    }

    public String getPay_way() {
        return pay_way;
    }

    public void setPay_way(String pay_way) {
        this.pay_way = pay_way;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public Double getMoney() {
        return money;
    }

    public void setMoney(Double money) {
        this.money = money;
    }

    @Override
    public String toString() {
        return id
                + "," + code
                + "," + rec_account
                + "," + rec_bank_name
                + "," + rec_name
                + "," + pay_account
                + "," + pay_name
                + "," + pay_comments
                + "," + pay_channel
                + "," + pay_way
                + "," + status
                + "," + timestamp
                + "," + money;
    }

    public static TransferRecord parse(String csvRow) {
        String[] fieldArray = csvRow.split(",");
        TransferRecord tr = new TransferRecord();
        tr.setId(fieldArray[0]);
        tr.setCode(fieldArray[1]);
        tr.setRec_account(fieldArray[2]);
        tr.setRec_bank_name(fieldArray[3]);
        tr.setRec_name(fieldArray[4]);
        tr.setPay_account(fieldArray[5]);
        tr.setPay_name(fieldArray[6]);
        tr.setPay_comments(fieldArray[7]);
        tr.setPay_channel(fieldArray[8]);
        tr.setPay_way(fieldArray[9]);
        tr.setStatus(fieldArray[10]);
        tr.setTimestamp(fieldArray[11]);
        tr.setMoney(Double.parseDouble(fieldArray[12]));
        return tr;
    }
}
