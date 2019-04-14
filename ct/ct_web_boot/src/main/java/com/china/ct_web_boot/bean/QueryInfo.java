package com.china.ct_web_boot.bean;

import lombok.Data;

/**
 * 该类用于存放用户请求的数据
 */
@Data
public class QueryInfo {
    private String telephone;
    private String year;
    private String month;
    private String day;

    public QueryInfo() {
        super();
    }

    public QueryInfo(String telephone, String year, String month, String day) {
        super();
        this.telephone = telephone;
        this.year = year;
        this.month = month;
        this.day = day;
    }
}
