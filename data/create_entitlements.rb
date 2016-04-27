class CreateEntitlements < Migration
  def up
    cql = <<-TABLE_CQL
      CREATE TABLE entitlements (
        guid VARCHAR,
        end_date timestamp,
        start_date timestamp,
        type VARCHAR,
        brand VARCHAR,
        product VARCHAR,
        PRIMARY KEY ((guid), end_date)
      ) WITH compression = { 'sstable_compression' : 'LZ4Compressor' };
    TABLE_CQL
    execute(cql)
  end

  def down
    cql = <<-TABLE_CQL
      DROP TABLE IF EXISTS entitlements;
    TABLE_CQL
    execute(cql)
  end
end
