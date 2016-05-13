require 'cassandra'
require './config/config.rb'
require './lib/migration.rb'

$logger = Logger.new(Cfg.config['logFile'], 'daily')

desc 'create db'

task :create do
  m = Migration.new
  cql = <<-KEYSPACE_CQL
            CREATE KEYSPACE IF NOT EXISTS #{Cfg.config['cassandraCluster']['keyspace']}
            WITH replication = {
              'class': 'SimpleStrategy',
              'replication_factor': 1
            }
  KEYSPACE_CQL

  m.execute(cql, false)

  cql = <<-KEYSPACE_CQL
          CREATE TABLE #{Cfg.config['tables']['history_migration']} (
            filename VARCHAR,
            PRIMARY KEY (filename)
          )
  KEYSPACE_CQL

  m.execute(cql)
end

desc 'migrate'
task :migrate do
  migrate_all
end

def migrate_all
  puts 'Running all migrations'
  puts Dir.glob("#{File.dirname(__FILE__)}/data/*.rb").inspect
  Dir.glob("#{File.dirname(__FILE__)}/data/*.rb").sort.each do |migration|
    filename = File.basename(migration).sub('.rb','')
    migration_record = Migration.new.execute("select * from #{Cfg.config['tables']['history_migration']} where filename = '#{filename}'")
    next if migration_record.size > 0
    Migration.new.execute("insert into #{Cfg.config['tables']['history_migration']} (filename) values ('#{filename}')")
    migration_class = filename.sub(/\d+_/,'').split('_').map(&:capitalize).join
    Module.module_eval(File.read(migration))
    print "Running #{migration_class}..."
    migration_class = Module.const_get(migration_class)
    puts 'done'
    migration_class.new.migrate
  end
end

desc 'delete'
task :delete do
  @connection ||= Connection.instance.connection
  @connection.execute("DROP TABLE IF EXISTS #{Cfg.config['cassandraCluster']['keyspace']}.#{Cfg.config['tables']['tc']}")
  @connection.execute("DROP TABLE IF EXISTS #{Cfg.config['cassandraCluster']['keyspace']}.#{Cfg.config['tables']['history_entitlements']}")
  @connection.execute("DROP MATERIALIZED VIEW IF EXISTS #{Cfg.config['cassandraCluster']['keyspace']}.#{Cfg.config['tables']['entitlements_by_enddate']}")
  @connection.execute("DROP TABLE IF EXISTS #{Cfg.config['cassandraCluster']['keyspace']}.#{Cfg.config['tables']['entitlements']}")
#  @connection.execute("DROP KEYSPACE IF EXISTS #{Cfg.config['cassandraCluster']['keyspace']}.#{Cfg.config['tables']['entitlements']}")
end
