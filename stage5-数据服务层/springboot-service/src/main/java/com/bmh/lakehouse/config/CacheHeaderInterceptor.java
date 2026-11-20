package com.bmh.lakehouse.config;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * X-Cache 探针初始化拦截器：preHandle 置探针为 HIT（假设 @Cacheable 命中）；
 * Controller 方法体（仅缓存未命中时才执行）内 CacheProbe.miss() 翻转为 MISS。
 * 响应头由 {@link CacheHeaderAdvice} 在 body 写出前写入。
 */
@Component
public class CacheHeaderInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        request.setAttribute(CacheProbe.ATTR, "HIT");
        return true;
    }
}
