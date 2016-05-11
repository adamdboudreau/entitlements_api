class CreateEntitlements < Migration
  def up
    cql = <<-TABLE_CQL
      CREATE TABLE #{Cfg.config['tables']['entitlements']} (
        guid TEXT,
        brand TEXT,
        source TEXT,
        product TEXT,
        trace_id TEXT,
        start_date TIMESTAMP,
        end_date TIMESTAMP,
        PRIMARY KEY ((guid, brand), source, product, trace_id, end_date)
      ) WITH compression = { 'sstable_compression' : 'LZ4Compressor' };
    TABLE_CQL
    execute(cql)

    cql = <<-TABLE_CQL
      CREATE TABLE #{Cfg.config['tables']['history_entitlements']} (
        guid TEXT,
        source TEXT,
        brand TEXT,
        product TEXT,
        trace_id TEXT,
        start_date TIMESTAMP,
        end_date TIMESTAMP,
        archive_date TIMESTAMP,
        archive_type TEXT,
        PRIMARY KEY ((guid, brand), source, product, trace_id, archive_date)
      ) WITH compression = { 'sstable_compression' : 'LZ4Compressor' };
    TABLE_CQL
    execute(cql)

    cql = <<-TABLE_CQL
      CREATE TABLE #{Cfg.config['tables']['tc']} (
        guid TEXT,
        brand TEXT,
        tc_version TEXT,
        tc_acceptance_date TIMESTAMP,
        PRIMARY KEY (guid, brand)
      ) WITH compression = { 'sstable_compression' : 'LZ4Compressor' };
    TABLE_CQL
    execute(cql)

    cql = "CREATE MATERIALIZED VIEW #{Cfg.config['tables']['entitlements_by_enddate']}" +
" AS SELECT * FROM #{Cfg.config['tables']['entitlements']} " +
" WHERE end_date IS NOT NULL AND guid IS NOT NULL AND brand IS NOT NULL " + 
"AND source IS NOT NULL AND product IS NOT NULL AND trace_id IS NOT NULL " +
" PRIMARY KEY ((guid, brand, source, product, trace_id), end_date) " 
#{}"WITH CLUSTERING ORDER BY (end_date ASC)"
    execute(cql)
  end

  def down
    execute("DROP TABLE IF EXISTS #{Cfg.config['tables']['tc']}")
    execute("DROP TABLE IF EXISTS #{Cfg.config['tables']['history_entitlements']}")
    execute("DROP TABLE IF EXISTS #{Cfg.config['tables']['entitlements']}")
  end
end
