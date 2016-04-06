class CreateEntitlements < Migration
  def up
    cql = <<-TABLE_CQL
      CREATE TABLE entitlements (
        guid VARCHAR,
        type VARCHAR,
        brand VARCHAR,
        product VARCHAR,
        start_date timestamp,
        end_date timestamp,
        PRIMARY KEY (guid)
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
