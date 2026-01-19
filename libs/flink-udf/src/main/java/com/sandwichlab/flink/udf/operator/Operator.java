package com.sandwichlab.flink.udf.operator;

import com.sandwichlab.flink.udf.config.OperatorConfig;

import java.io.Serializable;
import java.util.Map;

/**
 * Operator 接口
 *
 * 定义外部操作的通用接口，支持多种存储后端
 */
public interface Operator extends Serializable, AutoCloseable {

    /**
     * 初始化操作器
     *
     * @param config 配置
     */
    void open(OperatorConfig config);

    /**
     * 执行操作
     *
     * @param keys   主键字段
     * @param values 值字段
     * @return 操作是否成功
     */
    boolean execute(Map<String, Object> keys, Map<String, Object> values);

    /**
     * 刷新缓冲区（如果有）
     */
    default void flush() {
        // 默认空实现
    }

    /**
     * 获取操作器类型标识
     */
    String getType();
}
