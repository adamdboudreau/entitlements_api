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
              'replication_factor': 3
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

desc 'Lowercase brand, source, product'
task :lowercase do
  table_entitlements = "#{Cfg.config['cassandraCluster']['keyspace']}.#{Cfg.config['tables']['entitlements']}"
  puts "Lowercase rake task started, running SELECT * FROM #{table_entitlements}"
  @connection ||= Connection.instance.connection
  records = @connection.execute("SELECT * FROM #{table_entitlements}")
  nProcessed = 0
  nPassed = 0
  batch = @connection.batch do |batch|
    records.each do |row|
      if ((row['brand']==row['brand'].downcase) && (row['source']==row['source'].downcase) && (row['product']==row['product'].downcase)) 
        nPassed += 1
      else
        nProcessed += 1
        cql = "DELETE FROM #{table_entitlements} WHERE guid=? AND brand=? AND source=? AND product=? AND trace_id=? AND end_date=?"
        args = Array[row['guid'],row['brand'],row['source'],row['product'],row['trace_id'],row['end_date']]
        batch.add(cql, arguments: args)
        cql = "INSERT INTO #{table_entitlements} (guid, brand, end_date, source, product, trace_id, start_date) VALUES (?,?,?,?,?,?,?)"
        args = Array[row['guid'],row['brand'].downcase,row['end_date'],row['source'].downcase,row['product'].downcase,row['trace_id'],row['start_date']]
        batch.add(cql, arguments: args)
      end
      puts "#{nPassed+nProcessed} processed\n"
    end
  end
  @connection.execute(batch) if nProcessed>0
  puts "Lowercase procedure finished: #{nProcessed} records processed, #{nPassed} records passed"
end

desc 'Delete entitlements migrated from Zuora'
task :delete_zuora do |task, args|
  puts 'Zuora cleanup'
  nRecordsDeleted = Connection.instance.deleteZuora
  puts (nRecordsDeleted<0) ? 'Error happened during zuora deleting' : "Delete zuora entitlements task finished successfully, #{nRecordsDeleted} records have been deleted"
end

desc 'Migrate entitlements from Zuora'
task :zuora, [:rateplan, :guid, :billing, :autorenew, :subID] do |task, args|
  # qa file format: rake zuora[0,5,4,6,7]
  # prod file format: rake zuora[1,11,10,17,22] [27,4,0,10,1]
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
      billing = 'direct bill' # row[args[:billing].to_i]
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

desc 'SPDR run'
task :spdr do
  csvFile = ENV['GUID_CSV']
  abort "File not found: #{csvFile}" unless csvFile && File.file?(csvFile)
  nCounter = 0
  now = Time.now.to_i
  sFileName = "SPDR_check_#{now}.csv"

  pem = File.read(Cfg.config['campAPI']['pemFile'])
  key = ENV['CAMP_KEY']

  File.open(sFileName, "w") do |f|
    CSV.foreach(csvFile) do |row|
      if (nCounter>0)
        url = Cfg.config['campAPI']['url'] + row[4].strip
        puts "Processing line #{nCounter}: #{row}, URL: #{url}"
        uri = URI.parse(url)
        http = Net::HTTP.new(uri.host, uri.port)
        http.use_ssl = true
        http.ciphers = 'DEFAULT:!DH'
        http.cert = OpenSSL::X509::Certificate.new(pem)
        http.key = OpenSSL::PKey::RSA.new(key)
        http.verify_mode = OpenSSL::SSL::VERIFY_NONE
        response = http.request(Net::HTTP::Get.new(uri.request_uri)).body
#        puts "Got from SPDR: #{body}"
#        response = Hash.from_xml(body)
        f.write "#{row[4].strip}," + response.to_s.gsub("\n",'')[39..-1] + "\n"
      else
        f.write "#{row[4].strip},SPDR response\n"
      end
      nCounter += 1
    end
  end
end
