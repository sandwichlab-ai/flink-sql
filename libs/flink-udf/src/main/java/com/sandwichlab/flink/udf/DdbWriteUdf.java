package com.sandwichlab.flink.udf;

import com.sandwichlab.flink.udf.config.OperatorConfig;
import com.sandwichlab.flink.udf.operator.DdbOperator;
import com.sandwichlab.flink.udf.operator.Operator;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * DynamoDB 写入 UDF
 *
 * 在 Flink SQL 中间节点写入 DynamoDB，写入完成后数据继续流向下游。
 *
 * SQL 使用示例:
 * <pre>
 * SELECT
 *     *,
 *     ddb_write(
 *         fingerprint,     -- PK
 *         click_time,      -- SK
 *         user_id,
 *         click_id,
 *         click_id_name,
 *         utm_params       -- MAP 类型
 *     ) AS written
 * FROM raw_events;
 * </pre>
 *
 * 配置（通过环境变量或 Job Parameter）:
 * - DDB_TABLE_NAME: DynamoDB 表名
 * - AWS_REGION: AWS 区域（默认 us-west-2）
 */
public class DdbWriteUdf extends ScalarFunction {

    private static final Logger LOG = LoggerFactory.getLogger(DdbWriteUdf.class);

    private transient Operator operator;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        OperatorConfig config = OperatorConfig.fromContext(context);
        LOG.info("Initializing DdbWriteUdf with config: {}", config);

        this.operator = new DdbOperator();
        this.operator.open(config);
    }

    @Override
    public void close() throws Exception {
        if (operator != null) {
            operator.close();
        }
        super.close();
    }

    /**
     * 写入 DynamoDB 并返回 fingerprint（passthrough）
     *
     * @param fingerprint   用户指纹 (Partition Key)
     * @param clickTime     点击时间 (Sort Key)
     * @param userId        用户 ID
     * @param clickId       点击 ID
     * @param clickIdName   点击 ID 名称 (gclid, fbclid 等)
     * @param utmParams     UTM 参数 (MAP 类型)
     * @return fingerprint（透传，确保下游可以继续处理）
     */
    public String eval(
            String fingerprint,
            Long clickTime,
            String userId,
            String clickId,
            String clickIdName,
            Map<String, String> utmParams) {

        if (fingerprint == null || clickTime == null) {
            LOG.warn("Skipping write: fingerprint or clickTime is null");
            return fingerprint;
        }

        // 构建 keys
        Map<String, Object> keys = new HashMap<>();
        keys.put("fingerprint", fingerprint);
        keys.put("click_time", clickTime);

        // 构建 values
        Map<String, Object> values = new HashMap<>();
        if (userId != null) {
            values.put("user_id", userId);
        }
        if (clickId != null) {
            values.put("click_id", clickId);
        }
        if (clickIdName != null) {
            values.put("click_id_name", clickIdName);
        }
        if (utmParams != null && !utmParams.isEmpty()) {
            values.put("utm_json", utmParams);
        }

        // 执行写入
        boolean success = operator.execute(keys, values);
        if (!success) {
            LOG.warn("DDB write failed for fingerprint={}, clickTime={}", fingerprint, clickTime);
        }

        // 透传 fingerprint
        return fingerprint;
    }

    /**
     * 重载：不带 UTM 参数
     */
    public String eval(
            String fingerprint,
            Long clickTime,
            String userId,
            String clickId,
            String clickIdName) {
        return eval(fingerprint, clickTime, userId, clickId, clickIdName, null);
    }
}
