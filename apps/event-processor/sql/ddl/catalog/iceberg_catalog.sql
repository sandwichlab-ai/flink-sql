-- ============================================================
-- Iceberg Catalog (Glue Catalog + S3FileIO)
-- 使用 s3:// scheme - Athena 兼容
-- 跨账户配置：Flink (Dev-US) -> Glue (Data Platform)
-- ============================================================

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'warehouse' = 's3://dataware-landing-zone-dev/iceberg/',
    'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    'glue.id' = '809574936716'
)
