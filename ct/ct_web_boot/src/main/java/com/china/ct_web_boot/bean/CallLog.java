package com.china.ct_web_boot.bean;

import lombok.Data;

/**
 * 用于存放返回给用户的数据
 */
@Data
public class CallLog {
    private String id_date_contact;
    private String id_date_dimension;
    private String id_contact;
    private String call_sum;
    private String call_duration_sum;
    private String telephone;
    private String name;
    private String year;
    private String month;
    private String day;

    @Override
    public String toString() {
        return "CallLog{" +
                "call_sum='" + call_sum + '\'' +
                ", call_duration_sum='" + call_duration_sum + '\'' +
                ", telephone='" + telephone + '\'' +
                ", name='" + name + '\'' +
                ", year='" + year + '\'' +
                ", month='" + month + '\'' +
                ", day='" + day + '\'' +
                '}';
    }
}
