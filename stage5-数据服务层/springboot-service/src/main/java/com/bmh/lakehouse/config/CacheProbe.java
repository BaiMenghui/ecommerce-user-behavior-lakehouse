package com.bmh.lakehouse.config;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * X-Cache 探测工具。
 * 实现方式说明：
 *  - CacheHeaderInterceptor 在 preHandle 阶段把探针置为 "HIT"（假设 @Cacheable 会命中）；
 *  - Controller 方法体第一行调用 {@link #miss()}（方法体被执行即意味着缓存未命中），探针翻转为 "MISS"；
 *  - afterCompletion 阶段读探针写入响应头 X-Cache: HIT/MISS。
 *  无需 AOP，即可在保留 @Cacheable 注解的前提下拿到真实命中情况。
 */
public final class CacheProbe {

    public static final String ATTR = "CACHE_PROBE_ATTR";

    private CacheProbe() {
    }

    /** Controller 方法体内调用：标记本次为缓存未命中 */
    public static void miss() {
        ServletRequestAttributes ra =
                (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (ra != null) {
            ra.getRequest().setAttribute(ATTR, "MISS");
        }
    }

    public static String current(HttpServletRequest request) {
        Object v = request.getAttribute(ATTR);
        return v == null ? "HIT" : v.toString();
    }
}
