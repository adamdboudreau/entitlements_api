class CreateEntitlements < Migration
  def up
    cql = <<-TABLE_CQL
      CREATE TABLE #{Cfg.config['tables']['entitlements']} (
        id TIMEUUID,
        guid TEXT,
        brand TEXT,
        source TEXT,
        product TEXT,
        trace_id TEXT,
        start_date TIMESTAMP,
        end_date TIMESTAMP,
        PRIMARY KEY ((guid, brand), end_date, id)
      ) WITH compression = { 'sstable_compression' : 'LZ4Compressor' };
    TABLE_CQL
    execute(cql)

    cql = <<-TABLE_CQL
      CREATE TABLE #{Cfg.config['tables']['history_entitlements']} (
        id TIMEUUID,
        guid TEXT,
        source TEXT,
        brand TEXT,
        product TEXT,
        trace_id TEXT,
        start_date TIMESTAMP,
        end_date TIMESTAMP,
        archive_date TIMESTAMP,
        archive_type TEXT,
        PRIMARY KEY ((guid, brand), id)
      ) WITH compression = { 'sstable_compression' : 'LZ4Compressor' };
    TABLE_CQL
    execute(cql)

    cql = <<-TABLE_CQL
      CREATE TABLE #{Cfg.config['tables']['tc']} (
        guid TEXT,
        brand TEXT,
        tc_version FLOAT,
        tc_acceptance_date TIMESTAMP,
        PRIMARY KEY (guid, brand)
      ) WITH compression = { 'sstable_compression' : 'LZ4Compressor' };
    TABLE_CQL
    execute(cql)

    cql = "CREATE MATERIALIZED VIEW #{Cfg.config['tables']['entitlements_by_enddate']}" +
" AS SELECT end_date, id, guid, source, brand, product, trace_id, start_date FROM #{Cfg.config['tables']['entitlements']} " +
" WHERE end_date IS NOT NULL AND guid IS NOT NULL AND brand IS NOT NULL " + 
"AND id IS NOT NULL PRIMARY KEY (end_date, id, guid, brand)"
#    execute(cql)
  end

  def down
    execute("DROP TABLE IF EXISTS #{Cfg.config['tables']['tc']}")
    execute("DROP TABLE IF EXISTS #{Cfg.config['tables']['history_entitlements']}")
    execute("DROP TABLE IF EXISTS #{Cfg.config['tables']['entitlements']}")
  end
end
