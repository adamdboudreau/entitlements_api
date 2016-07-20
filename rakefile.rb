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
  @connection.execute("DROP KEYSPACE IF EXISTS #{Cfg.config['cassandraCluster']['keyspace']}")
end

desc 'archive'
task :archive do
  puts 'Archiving rake task started'
  nRecordsArchived = Connection.instance.postArchive
  puts (nRecordsArchived<0) ? 'Error happened during archiving' : "Archiving rake task finished successfully, #{nRecordsArchived} records have been archived"
end

desc 'Migrate entitlements from Zuora'
task :delete_zuora do |task, args|
  puts 'Zuora cleanup'
  nRecordsDeleted = Connection.instance.deleteZuora
  puts (nRecordsDeleted<0) ? 'Error happened during zuora deleting' : "Delete zuora entitlements task finished successfully, #{nRecordsDeleted} records have been deleted"
end

desc 'Migrate entitlements from Zuora'
task :zuora, [:rateplan, :guid, :billing, :autorenew, :subID] do |task, args|
  # qa file format: rake zuora[0,5,4,6,7]
  # prod file format: rake zuora[1,11,10,17,22]
  puts "Zuora migration, task=#{task}, args=#{args}"
  abort "Incorrect parameters: please use rake zuora[guidColumn, billingColumn, autorenewColumn, subIDColumn] format" unless args[:subID]
  csvFile = ENV['ZUORA_CSV']
  abort "File not found: #{csvFile}" unless csvFile && File.file?(csvFile)
  nCounter = 0
  now = Time.now.to_i
  # guid,brand,end_date,source,product,trace_id,start_date
  sLineEB1 = "<GUID>,gcl,2099-09-30 00:00:01+0000,zuora,fullgcl,<subID>,#{now}000"
  sLineEB2 = "<GUID>,gcl,2016-10-31 00:00:01+0000,zuora,wch,<subID>,#{now}000"
  sLineFrench = "<GUID>,gcl,2099-09-30 00:00:01+0000,zuora,frenchgcl,<subID>,#{now}000"
  sLineMonthly = "<GUID>,gcl,2099-09-30 00:00:01+0000,zuora,fullgcl,<subID>,#{now}000"
  sFileName = "temp_#{now}.csv"

  File.open(sFileName, "w") do |f|
    CSV.foreach(csvFile) do |row|
      puts "Processing line #{nCounter}: #{row}"
      billing = row[args[:billing].to_i]
      if billing && (billing.strip.downcase=='direct bill') then
        begin
          rateplan = row[args[:rateplan].to_i].strip.downcase
          guid = row[args[:guid].to_i].strip
          subID = row[args[:subID].to_i].strip
          autorenew = row[args[:autorenew].to_i].strip.downcase
          if (rateplan.include? 'french') then
            f.write "#{sLineFrench.gsub('<GUID>',guid).gsub('<subID>',subID)}\n"
          elsif ((rateplan.include? 'monthly') && (autorenew=='true')) then
            f.write "#{sLineMonthly.gsub('<GUID>',guid).gsub('<subID>',subID)}\n"
          elsif (autorenew=='true') then
            f.write "#{sLineEB1.gsub('<GUID>',guid).gsub('<subID>',subID)}\n"
            f.write "#{sLineEB2.gsub('<GUID>',guid).gsub('<subID>',subID)}\n"
          else
            puts "Skipping line #{nCounter}: #{row}"
          end
        rescue Exception => e
          puts "Skipped, error happened on line #{nCounter}: #{e.message}"
        end
      end
      nCounter += 1
    end
  end
  puts "#{nCounter} records imported into #{sFileName}"
end
