package com.bmh.lakehouse.config;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.core.MethodParameter;
import org.springframework.http.MediaType;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;

/**
 * X-Cache 响应头写入时机：@RestController 的 JSON body 在 handler 内部即被消息转换器写出，
 * preHandle/postHandle/afterCompletion 均晚于或早于正确时机的问题——
 * 只有 ResponseBodyAdvice.beforeBodyWrite 位于"方法体已执行完、body 尚未写出"的窗口，
 * 此时读探针（HIT=方法体未执行即缓存命中 / MISS=方法体执行即未命中）写响应头必然生效。
 */
@ControllerAdvice
public class CacheHeaderAdvice implements ResponseBodyAdvice<Object> {

    @Override
    public boolean supports(MethodParameter returnType, Class converterType) {
        return true;
    }

    @Override
    public Object beforeBodyWrite(Object body, MethodParameter returnType, MediaType selectedContentType,
                                  Class selectedConverterType, ServerHttpRequest request,
                                  ServerHttpResponse response) {
        ServletRequestAttributes ra =
                (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (ra != null) {
            HttpServletRequest req = ra.getRequest();
            response.getHeaders().set("X-Cache", CacheProbe.current(req));
        }
        return body;
    }
}
