class CreateEntitlements < Migration
  def up
    cql = <<-TABLE_CQL
      CREATE TABLE #{Cfg.config['tables']['entitlements']} (
        id TIMEUUID PRIMARY KEY,
        guid VARCHAR,
        source VARCHAR,
        brand VARCHAR,
        product VARCHAR,
        subscription_id VARCHAR,
        start_date TIMESTAMP,
        end_date TIMESTAMP
      ) WITH compression = { 'sstable_compression' : 'LZ4Compressor' };
    TABLE_CQL
    execute(cql)

    cql = "CREATE MATERIALIZED VIEW #{Cfg.config['tables']['entitlements_by_guid']}" +
" AS SELECT guid, end_date, source, brand, product, start_date FROM #{Cfg.config['tables']['entitlements']} " +
" WHERE guid IS NOT NULL AND id IS NOT NULL PRIMARY KEY (id, guid)"
    execute(cql)

    cql = "CREATE MATERIALIZED VIEW #{Cfg.config['tables']['entitlements_by_enddate']}" +
" AS SELECT end_date, guid, source, brand, type, product, start_date FROM #{Cfg.config['tables']['entitlements']} " +
" WHERE guid IS NOT NULL AND id IS NOT NULL PRIMARY KEY (id, end_date)"
    execute(cql)
  end

  def down
    cql = <<-TABLE_CQL
      DROP TABLE IF EXISTS #{Cfg.config['tables']['entitlements']};
    TABLE_CQL
    execute(cql)
  end
end
