package com.sandwichlab.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * SQL 文件加载器
 *
 * 从 classpath 加载 SQL 文件，支持变量替换
 */
public class SqlLoader {

    private static final Logger LOG = LoggerFactory.getLogger(SqlLoader.class);
    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([^}]+)}");

    private final Map<String, String> variables;

    public SqlLoader(Map<String, String> variables) {
        this.variables = variables;
    }

    /**
     * 从 classpath 加载 SQL 文件
     *
     * @param resourcePath SQL 文件路径，例如 "sql/ddl/source_raw_events.sql"
     * @return 替换变量后的 SQL 内容
     */
    public String load(String resourcePath) {
        LOG.info("Loading SQL from: {}", resourcePath);

        String rawSql = readResource(resourcePath);
        // 先移除注释，再替换变量（避免注释中的变量被解析）
        String noComments = removeComments(rawSql);
        String sql = replaceVariables(noComments);

        LOG.debug("Loaded SQL:\n{}", sql);
        return sql;
    }

    private String readResource(String resourcePath) {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new RuntimeException("SQL file not found: " + resourcePath);
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                return reader.lines().collect(Collectors.joining("\n"));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read SQL file: " + resourcePath, e);
        }
    }

    private String replaceVariables(String sql) {
        Matcher matcher = VARIABLE_PATTERN.matcher(sql);
        StringBuffer result = new StringBuffer();

        while (matcher.find()) {
            String varName = matcher.group(1);
            String value = variables.get(varName);
            if (value == null) {
                throw new RuntimeException("Variable not found: ${" + varName + "}");
            }
            matcher.appendReplacement(result, Matcher.quoteReplacement(value));
        }
        matcher.appendTail(result);

        return result.toString();
    }

    private String removeComments(String sql) {
        // 移除单行注释 (-- ...) - 使用多行模式
        String noLineComments = sql.replaceAll("(?m)--.*$", "");

        // 移除多行注释 (/* ... */)
        String noBlockComments = noLineComments.replaceAll("(?s)/\\*.*?\\*/", "");

        // 清理多余空行
        return noBlockComments
                .replaceAll("(?m)^\\s*$[\r\n]*", "")
                .trim();
    }
}
