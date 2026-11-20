package com.bmh.lakehouse;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

/**
 * 电商用户行为湖仓分析平台 - 阶段5 数据服务层入口。
 */
@SpringBootApplication
@EnableCaching
public class LakehouseApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(LakehouseApiApplication.class, args);
    }
}
