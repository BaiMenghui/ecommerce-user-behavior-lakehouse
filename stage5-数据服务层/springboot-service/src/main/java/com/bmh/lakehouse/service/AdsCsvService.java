package com.bmh.lakehouse.service;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * ADS 指标 CSV 数据服务：启动时扫描 app.data-dir 下 5 个 ADS 目录，
 * 按首行表头解析为 List&lt;Map&lt;String,String&gt;&gt;（不按列位置硬编码）。
 */
@Service
public class AdsCsvService {

    private static final Logger log = LoggerFactory.getLogger(AdsCsvService.class);

    public static final List<String> TABLES = List.of(
            "ads_dau_trend", "ads_funnel", "ads_item_topn", "ads_category_share", "ads_hour_heatmap");

    private final String dataDir;
    /** table -> rows（每行为 列名->值） */
    private final Map<String, List<Map<String, String>>> data = new LinkedHashMap<>();

    public AdsCsvService(@Value("${app.data-dir}") String dataDir) {
        this.dataDir = dataDir;
    }

    @PostConstruct
    public void load() throws IOException {
        Path root = Paths.get(dataDir);
        for (String table : TABLES) {
            Path dir = root.resolve(table);
            List<Map<String, String>> rows = new ArrayList<>();
            if (!Files.isDirectory(dir)) {
                log.warn("ADS 目录不存在: {}", dir);
            } else {
                try (Stream<Path> files = Files.list(dir)) {
                    List<Path> parts = files
                            .filter(p -> p.getFileName().toString().startsWith("part-")
                                    && p.getFileName().toString().endsWith(".csv"))
                            .sorted().toList();
                    for (Path part : parts) {
                        rows.addAll(readCsv(part));
                    }
                }
            }
            data.put(table, rows);
            log.info("载入 {} {} 行", table, rows.size());
        }
        log.info("ADS 数据载入完成: {} 张表, 共 {} 行, dt 范围 {} ~ {}",
                TABLES.size(), TABLES.stream().mapToInt(t -> data.get(t).size()).sum(), minDt(), latestDt());
    }

    /** 读取单个 CSV 文件：首行表头，strip BOM，容忍尾部空列 */
    private List<Map<String, String>> readCsv(Path file) throws IOException {
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        List<Map<String, String>> rows = new ArrayList<>();
        if (lines.isEmpty()) {
            return rows;
        }
        String headerLine = stripBom(lines.get(0));
        String[] headers = headerLine.split(",", -1);
        for (int i = 1; i < lines.size(); i++) {
            String line = stripBom(lines.get(i));
            if (line.isBlank()) {
                continue;
            }
            // 容忍行尾多余空列：如 "a,b,c,," 之类；字段值本身不含逗号
            String trimmed = line.replaceAll("(,\\s*)+$", "");
            String[] values = trimmed.split(",", -1);
            Map<String, String> row = new LinkedHashMap<>();
            for (int c = 0; c < headers.length; c++) {
                row.put(headers[c].trim(), c < values.length ? values[c].trim() : "");
            }
            if (row.get("dt") != null && !row.get("dt").isBlank()) {
                rows.add(row);
            }
        }
        return rows;
    }

    private String stripBom(String s) {
        return s.startsWith("\uFEFF") ? s.substring(1) : s;
    }

    public String getDataDir() {
        return dataDir;
    }

    public int tableCount() {
        return TABLES.size();
    }

    /** 全表行 */
    public List<Map<String, String>> rows(String table) {
        return data.getOrDefault(table, List.of());
    }

    /** 按 dt 过滤；dt 为空返回全部 */
    public List<Map<String, String>> rowsByDt(String table, String dt) {
        List<Map<String, String>> all = rows(table);
        if (dt == null || dt.isBlank()) {
            return all;
        }
        return all.stream().filter(r -> dt.equals(r.get("dt"))).toList();
    }

    /** 单日单行（该表按 dt 恰好一行的场景：dau_trend / funnel / hour_heatmap） */
    public Map<String, String> oneRowByDt(String table, String dt) {
        List<Map<String, String>> list = rowsByDt(table, dt);
        return list.isEmpty() ? null : list.get(0);
    }

    /** 数据里最新一天（dt 缺省值） */
    public String latestDt() {
        TreeSet<String> dts = allDtSet();
        return dts.isEmpty() ? null : dts.last();
    }

    public String minDt() {
        TreeSet<String> dts = allDtSet();
        return dts.isEmpty() ? null : dts.first();
    }

    private TreeSet<String> allDtSet() {
        TreeSet<String> dts = new TreeSet<>();
        for (Map<String, String> r : rows("ads_dau_trend")) {
            dts.add(r.get("dt"));
        }
        return dts;
    }

    /** 可用日期列表（升序） */
    public List<String> dates() {
        return new ArrayList<>(allDtSet());
    }

    // ---- 数值转换 helper ----

    public static long asLong(Map<String, String> row, String col) {
        String v = row.get(col);
        if (v == null || v.isBlank() || "null".equalsIgnoreCase(v)) {
            return 0L;
        }
        return (long) Double.parseDouble(v.trim());
    }

    public static double asDouble(Map<String, String> row, String col) {
        String v = row.get(col);
        if (v == null || v.isBlank() || "null".equalsIgnoreCase(v)) {
            return 0.0;
        }
        return Double.parseDouble(v.trim());
    }

    public static String asStr(Map<String, String> row, String col) {
        String v = row.get(col);
        return v == null ? "" : v;
    }

    /** 保留 n 位小数 */
    public static double round(double v, int n) {
        double p = Math.pow(10, n);
        return Math.round(v * p) / p;
    }

    @SuppressWarnings("unused")
    private static Comparator<Map<String, String>> byColDesc(String col) {
        return Comparator.comparingDouble((Map<String, String> r) -> asDouble(r, col)).reversed();
    }
}
