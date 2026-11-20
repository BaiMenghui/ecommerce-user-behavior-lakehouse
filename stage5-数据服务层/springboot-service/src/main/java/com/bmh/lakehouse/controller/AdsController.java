package com.bmh.lakehouse.controller;

import com.bmh.lakehouse.config.CacheConfig;
import com.bmh.lakehouse.config.CacheProbe;
import com.bmh.lakehouse.service.AdsCsvService;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * ADS 指标 RESTful 接口（均返回 {code:0,msg:"ok",data:...}）。
 * dt 缺省时取数据里最新一天；响应头 X-Cache: HIT/MISS 标注缓存命中情况。
 */
@RestController
public class AdsController {

    private final AdsCsvService ads;

    public AdsController(AdsCsvService ads) {
        this.ads = ads;
    }

    /** 1. 可用日期列表（取 ads_dau_trend 的 dt，升序） */
    @Cacheable(cacheNames = "ads_dates", key = "'all'")
    @GetMapping("/api/dates")
    public ApiResponse dates() {
        CacheProbe.miss();
        return ApiResponse.ok(ads.dates());
    }

    /** 2. 单日 KPI 汇总：DAU/新增/老客/购买人数/订单/GMV/客单价 */
    @Cacheable(cacheNames = "ads_kpi", key = "#dt == null ? 'latest' : #dt")
    @GetMapping("/api/kpi/summary")
    public ApiResponse kpiSummary(@RequestParam(required = false) String dt) {
        CacheProbe.miss();
        String day = resolveDt(dt);
        Map<String, String> row = ads.oneRowByDt("ads_dau_trend", day);
        if (row == null) {
            return ApiResponse.error("no data for dt=" + day);
        }
        Map<String, Object> kpi = new LinkedHashMap<>();
        kpi.put("dt", day);
        kpi.put("dau", AdsCsvService.asLong(row, "dau"));
        kpi.put("new_user_cnt", AdsCsvService.asLong(row, "new_user_cnt"));
        kpi.put("old_user_cnt", AdsCsvService.asLong(row, "old_user_cnt"));
        kpi.put("new_user_ratio", AdsCsvService.asDouble(row, "new_user_ratio"));
        kpi.put("paying_user_cnt", AdsCsvService.asLong(row, "paying_user_cnt"));
        kpi.put("order_cnt", AdsCsvService.asLong(row, "order_cnt"));
        kpi.put("gmv", AdsCsvService.asDouble(row, "gmv"));
        kpi.put("avg_ticket", AdsCsvService.asDouble(row, "avg_ticket"));
        return ApiResponse.ok(kpi);
    }

    /** 3. DAU 趋势：全部日期的 [dt, dau, gmv, order_cnt]（折线用） */
    @Cacheable(cacheNames = "ads_dau_trend", key = "'all'")
    @GetMapping("/api/dau-trend")
    public ApiResponse dauTrend() {
        CacheProbe.miss();
        List<Map<String, Object>> list = new ArrayList<>();
        for (Map<String, String> row : ads.rows("ads_dau_trend")) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("dt", AdsCsvService.asStr(row, "dt"));
            item.put("dau", AdsCsvService.asLong(row, "dau"));
            item.put("gmv", AdsCsvService.asDouble(row, "gmv"));
            item.put("order_cnt", AdsCsvService.asLong(row, "order_cnt"));
            list.add(item);
        }
        list.sort((a, b) -> String.valueOf(a.get("dt")).compareTo(String.valueOf(b.get("dt"))));
        return ApiResponse.ok(list);
    }

    /** 4. 单日漏斗：pv/fav/cart/buy + 三个转化率（pv2cart/cart2buy/pv2buy） */
    @Cacheable(cacheNames = "ads_funnel", key = "#dt == null ? 'latest' : #dt")
    @GetMapping("/api/funnel")
    public ApiResponse funnel(@RequestParam(required = false) String dt) {
        CacheProbe.miss();
        String day = resolveDt(dt);
        Map<String, String> row = ads.oneRowByDt("ads_funnel", day);
        if (row == null) {
            return ApiResponse.error("no data for dt=" + day);
        }
        Map<String, Object> funnel = new LinkedHashMap<>();
        funnel.put("dt", day);
        funnel.put("pv", AdsCsvService.asLong(row, "pv_cnt"));
        funnel.put("fav", AdsCsvService.asLong(row, "fav_cnt"));
        funnel.put("cart", AdsCsvService.asLong(row, "cart_cnt"));
        funnel.put("buy", AdsCsvService.asLong(row, "buy_cnt"));
        funnel.put("pv2cart_rate", AdsCsvService.asDouble(row, "pv2cart_rate"));
        funnel.put("cart2buy_rate", AdsCsvService.asDouble(row, "cart2buy_rate"));
        funnel.put("pv2buy_rate", AdsCsvService.asDouble(row, "pv2buy_rate"));
        return ApiResponse.ok(funnel);
    }

    /** 5. 单日 Top10 商品（按 rank_no 升序） */
    @Cacheable(cacheNames = "ads_topn", key = "#dt == null ? 'latest' : #dt")
    @GetMapping("/api/topn")
    public ApiResponse topn(@RequestParam(required = false) String dt) {
        CacheProbe.miss();
        String day = resolveDt(dt);
        List<Map<String, Object>> list = new ArrayList<>();
        for (Map<String, String> row : ads.rowsByDt("ads_item_topn", day)) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("rank_no", AdsCsvService.asLong(row, "rank_no"));
            item.put("item_id", AdsCsvService.asStr(row, "item_id"));
            item.put("category_id", AdsCsvService.asStr(row, "category_id"));
            item.put("buy_cnt", AdsCsvService.asLong(row, "buy_cnt"));
            item.put("action_cnt", AdsCsvService.asLong(row, "action_cnt"));
            item.put("buy_user_cnt", AdsCsvService.asLong(row, "buy_user_cnt"));
            item.put("gmv", AdsCsvService.asDouble(row, "gmv"));
            list.add(item);
        }
        list.sort((a, b) -> Long.compare((long) a.get("rank_no"), (long) b.get("rank_no")));
        if (list.isEmpty()) {
            return ApiResponse.error("no data for dt=" + day);
        }
        return ApiResponse.ok(list);
    }

    /** 6. 单日类目购买占比（buy_share_ratio 降序；limit 可选截断，缓存始终存全量） */
    @Cacheable(cacheNames = "ads_category_share", key = "#dt == null ? 'latest' : #dt")
    @GetMapping("/api/category-share")
    public ApiResponse categoryShare(@RequestParam(required = false) String dt,
                                     @RequestParam(required = false, defaultValue = "0") int limit) {
        CacheProbe.miss();
        String day = resolveDt(dt);
        List<Map<String, Object>> list = new ArrayList<>();
        for (Map<String, String> row : ads.rowsByDt("ads_category_share", day)) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("category_id", AdsCsvService.asStr(row, "category_id"));
            item.put("buy_cnt", AdsCsvService.asLong(row, "buy_cnt"));
            item.put("action_cnt", AdsCsvService.asLong(row, "action_cnt"));
            item.put("item_cnt", AdsCsvService.asLong(row, "item_cnt"));
            item.put("buy_share_ratio", AdsCsvService.asDouble(row, "buy_share_ratio"));
            item.put("action_share_ratio", AdsCsvService.asDouble(row, "action_share_ratio"));
            list.add(item);
        }
        list.sort((a, b) -> Double.compare((double) b.get("buy_share_ratio"), (double) a.get("buy_share_ratio")));
        if (list.isEmpty()) {
            return ApiResponse.error("no data for dt=" + day);
        }
        return ApiResponse.ok(limit > 0 ? list.subList(0, Math.min(limit, list.size())) : list);
    }

    /** 7. 单日 24 小时行为量（热力柱用） */
    @Cacheable(cacheNames = "ads_hour_heatmap", key = "#dt == null ? 'latest' : #dt")
    @GetMapping("/api/hour-heatmap")
    public ApiResponse hourHeatmap(@RequestParam(required = false) String dt) {
        CacheProbe.miss();
        String day = resolveDt(dt);
        List<Map<String, Object>> list = new ArrayList<>();
        for (Map<String, String> row : ads.rowsByDt("ads_hour_heatmap", day)) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("hour_of_day", AdsCsvService.asLong(row, "hour_of_day"));
            item.put("action_cnt", AdsCsvService.asLong(row, "action_cnt"));
            item.put("pv_cnt", AdsCsvService.asLong(row, "pv_cnt"));
            item.put("fav_cnt", AdsCsvService.asLong(row, "fav_cnt"));
            item.put("cart_cnt", AdsCsvService.asLong(row, "cart_cnt"));
            item.put("buy_cnt", AdsCsvService.asLong(row, "buy_cnt"));
            list.add(item);
        }
        list.sort((a, b) -> Long.compare((long) a.get("hour_of_day"), (long) b.get("hour_of_day")));
        if (list.isEmpty()) {
            return ApiResponse.error("no data for dt=" + day);
        }
        return ApiResponse.ok(list);
    }

    /** 8. 健康检查：状态/缓存类型/数据目录/表数量（不缓存） */
    @GetMapping("/api/health")
    public ApiResponse health() {
        CacheProbe.miss();
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("status", "UP");
        info.put("cacheType", CacheConfig.ACTIVE_CACHE_TYPE.get());
        info.put("dataDir", ads.getDataDir());
        info.put("tables", ads.tableCount());
        info.put("latestDt", ads.latestDt());
        return ApiResponse.ok(info);
    }

    private String resolveDt(String dt) {
        return dt == null || dt.isBlank() ? ads.latestDt() : dt;
    }
}
