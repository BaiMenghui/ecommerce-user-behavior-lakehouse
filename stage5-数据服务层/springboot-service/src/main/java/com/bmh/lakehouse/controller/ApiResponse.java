package com.bmh.lakehouse.controller;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 统一 JSON 响应包装：{code, msg, data}。
 * 注意：必须保留 @JsonCreator 反序列化构造——Redis 缓存读回时需要，
 * 缺失会导致缓存反序列化失败、每次请求都 MISS（caffeine 堆内缓存不受影响）。
 */
public class ApiResponse {

    private final int code;
    private final String msg;
    private final Object data;

    @JsonCreator
    public ApiResponse(@JsonProperty("code") int code,
                       @JsonProperty("msg") String msg,
                       @JsonProperty("data") Object data) {
        this.code = code;
        this.msg = msg;
        this.data = data;
    }

    public static ApiResponse ok(Object data) {
        return new ApiResponse(0, "ok", data);
    }

    public static ApiResponse error(String msg) {
        return new ApiResponse(1, msg, null);
    }

    public int getCode() {
        return code;
    }

    public String getMsg() {
        return msg;
    }

    public Object getData() {
        return data;
    }
}
