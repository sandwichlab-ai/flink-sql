#!/bin/bash
# 完整部署脚本 - 合并 SQL 并上传到 S3
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=========================================="
echo "Flink SQL 部署"
echo "=========================================="
echo "环境: ${ENV:-dev}"
echo "应用: ${APP_NAME}"
echo "S3:   s3://${CODE_BUCKET}/${SQL_KEY}"
echo "=========================================="

# 创建构建目录
BUILD_DIR="${PROJECT_DIR}/.build"
mkdir -p "${BUILD_DIR}"

# 合并 SQL 文件
COMBINED_SQL="${BUILD_DIR}/combined.sql"

echo "合并 SQL 文件..."
cat > "${COMBINED_SQL}" << 'HEADER'
-- ============================================================
-- Flink SQL Job
-- 自动生成于: $(date -u +"%Y-%m-%d %H:%M:%S UTC")
-- ============================================================

HEADER

# 添加 DDL 文件
echo "" >> "${COMBINED_SQL}"
echo "-- ==================== DDL ====================" >> "${COMBINED_SQL}"
for f in "${PROJECT_DIR}"/sql/ddl/*.sql; do
    if [ -f "$f" ]; then
        echo "  - 添加: $(basename "$f")"
        echo "" >> "${COMBINED_SQL}"
        echo "-- Source: $(basename "$f")" >> "${COMBINED_SQL}"
        cat "$f" >> "${COMBINED_SQL}"
    fi
done

# 添加 DML 文件 (只添加未注释的)
echo "" >> "${COMBINED_SQL}"
echo "-- ==================== DML ====================" >> "${COMBINED_SQL}"
for f in "${PROJECT_DIR}"/sql/dml/*.sql; do
    if [ -f "$f" ]; then
        echo "  - 添加: $(basename "$f")"
        echo "" >> "${COMBINED_SQL}"
        echo "-- Source: $(basename "$f")" >> "${COMBINED_SQL}"
        cat "$f" >> "${COMBINED_SQL}"
    fi
done

echo ""
echo "SQL 文件大小: $(wc -c < "${COMBINED_SQL}") bytes"

# 替换环境变量
echo "替换环境变量..."
envsubst < "${COMBINED_SQL}" > "${COMBINED_SQL}.tmp"
mv "${COMBINED_SQL}.tmp" "${COMBINED_SQL}"

# 上传到 S3
echo "上传到 S3..."
aws s3 cp "${COMBINED_SQL}" "s3://${CODE_BUCKET}/${SQL_KEY}" \
    --region "${AWS_REGION}" \
    --profile "${AWS_PROFILE}"

echo ""
echo "部署完成!"
echo ""
echo "验证:"
echo "  aws s3 ls s3://${CODE_BUCKET}/${SQL_KEY} --profile ${AWS_PROFILE}"
