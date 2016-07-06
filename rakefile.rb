require 'csv'

require './config/config.rb'
require './lib/migration.rb'

$logger = (Cfg.config[:env]=='dev') ? Logger.new(Cfg.config['logFile'], 'daily') : Le.new(Cfg.config['logEntriesToken'])

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
#  @connection.execute("DROP TABLE IF EXISTS #{Cfg.config['cassandraCluster']['keyspace']}.#{Cfg.config['tables']['tc']}")
#  @connection.execute("DROP TABLE IF EXISTS #{Cfg.config['cassandraCluster']['keyspace']}.#{Cfg.config['tables']['history_entitlements']}")
#  @connection.execute("DROP MATERIALIZED VIEW IF EXISTS #{Cfg.config['cassandraCluster']['keyspace']}.#{Cfg.config['tables']['entitlements_by_enddate']}")
#  @connection.execute("DROP TABLE IF EXISTS #{Cfg.config['cassandraCluster']['keyspace']}.#{Cfg.config['tables']['entitlements']}")
  @connection.execute("DROP KEYSPACE IF EXISTS #{Cfg.config['cassandraCluster']['keyspace']}")
end

desc 'archive'
task :archive do
  puts 'Archiving rake task started'
  nRecordsArchived = Connection.instance.postArchive
  puts 'Error happened during archiving' if nRecordsArchived<0
  puts "Archiving rake task finished successfully, #{nRecordsArchived} records have been archived"
end

desc 'Migrate entitlements from Zuora'
task :zuora do |task, args|
  puts 'Zuora migration'
  csvFile = ENV['ZUORA_CSV']
  abort "File not found: #{csvFile}" unless File.file?(csvFile)
  nCounter = 0
  sLine = '<guid>,gcl,1506729601,zuora,fullgcl,<guid>,1472688001' # guid,brand,end_date,source,product,trace_id,start_date
  sLine = '<guid>,gcl,2017-09-01 00:00:01+0000,zuora,fullgcl,<guid>,2017-09-30 00:00:01+0000' # guid,brand,end_date,source,product,trace_id,start_date
  sLine = '<guid>,gcl,2017-10-01 00:00:01+0000,zuora,frenchgcl,<guid>,2017-09-30 00:00:01+0000' # guid,brand,end_date,source,product,trace_id,start_date
  sFileName = "temp_#{Time.now.to_i}.csv"

  File.open(sFileName, "w") do |f|
    CSV.foreach(csvFile) do |row|
#      product, guid, start_date = row.to_s.split ','
      if nCounter>0 then
        begin
          sInsert = sLine.gsub('<guid>',row[11])
          if (row[1].downcase.include? 'french') then
            f.write "#{sInsert}\n"
          elsif (row[1].downcase.include? 'monthly') then
#            f.write sInsert.gsub('2017-09-30 00:00:01','2099-09-30 00:00:01') + "\n"
          else
#            f.write "#{sInsert}\n"
#            f.write "#{sInsert.gsub('fullgcl','wch')}\n"
          end
        rescue 
          puts "Error happened on line #{nCounter}, skipped"
        end
      end
      nCounter += 1
    end
  end
  puts "#{nCounter} records imported into #{sFileName}"
end
