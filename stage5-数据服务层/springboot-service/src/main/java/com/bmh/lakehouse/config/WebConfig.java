package com.bmh.lakehouse.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * MVC 配置：对 /api/** 注册 X-Cache 拦截器。
 */
@Configuration
public class WebConfig implements WebMvcConfigurer {

    private final CacheHeaderInterceptor cacheHeaderInterceptor;

    public WebConfig(CacheHeaderInterceptor cacheHeaderInterceptor) {
        this.cacheHeaderInterceptor = cacheHeaderInterceptor;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(cacheHeaderInterceptor).addPathPatterns("/api/**");
    }
}
