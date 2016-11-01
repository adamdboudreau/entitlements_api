require 'csv'
require 'aws-sdk'
require 'zlib'

require './config/config.rb'
require './lib/migration.rb'

# $logger = (Cfg.config[:env]=='dev') ? Logger.new(Cfg.config['logFile'], 'daily') : Le.new(Cfg.config['logEntriesToken'])

#######################################################################################

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

#######################################################################################

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

#######################################################################################

desc 'delete'
task :delete do
  @connection ||= Connection.instance.connection
  @connection.execute("DROP KEYSPACE IF EXISTS #{Cfg.config['cassandraCluster']['keyspace']}")
end

#######################################################################################

desc 'archive'
task :archive, [:limit] do |task, args|
  puts "Archiving rake task started with limit=#{args[:limit]}"
  nRecordsArchived = Connection.instance.postArchive args[:limit]
  puts (nRecordsArchived<0) ? 'Error happened during archiving' : "Archiving rake task finished successfully, #{nRecordsArchived} records have been archived"
end

#######################################################################################

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

#######################################################################################

desc 'Delete entitlements migrated from Zuora'
task :delete_zuora do |task, args|
  puts 'Zuora cleanup'
  nRecordsDeleted = Connection.instance.deleteZuora
  puts (nRecordsDeleted<0) ? 'Error happened during zuora deleting' : "Delete zuora entitlements task finished successfully, #{nRecordsDeleted} records have been deleted"
end

#######################################################################################

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
  sLineEB1 = "<GUID>,gcl,2099-12-31 00:00:01+0000,zuora,fullgcl,<subID>,#{now}000"
  sLineEB2 = "<GUID>,gcl,2016-10-31 00:00:01+0000,zuora,wch,<subID>,#{now}000"
  sLineFrench = "<GUID>,gcl,2099-09-30 00:00:01+0000,zuora,frenchgcl,<subID>,#{now}000"
  sLineMonthly = "<GUID>,gcl,2099-09-30 00:00:01+0000,zuora,fullgcl,<subID>,#{now}000"
  sFileName = "temp_#{now}.csv"

  File.open(sFileName, "w") do |f|
    CSV.foreach(csvFile) do |row|
      puts "Processing line #{nCounter}: #{row}"
      billing = (args[:billing].to_i < 0 ) ? 'direct bill' : row[args[:billing].to_i]
      if billing && (billing.strip.downcase=='direct bill') then
        begin
          rateplan = (args[:rateplan].to_i < 0 ) ? '' : row[args[:rateplan].to_i].strip.downcase
          guid = row[args[:guid].to_i].strip
          subID = row[args[:subID].to_i].strip
          autorenew = (args[:autorenew].to_i < 0 ) ? 'true' : row[args[:autorenew].to_i].strip.downcase
          if (rateplan.include? 'french') then
            f.write "#{sLineFrench.gsub('<GUID>',guid).gsub('<subID>',subID)}\n"
          elsif ((rateplan.include? 'monthly') && (autorenew=='true')) then
            f.write "#{sLineMonthly.gsub('<GUID>',guid).gsub('<subID>',subID)}\n"
          elsif (autorenew=='true') then
            f.write "#{sLineEB1.gsub('<GUID>',guid).gsub('<subID>',subID)}\n"
            #f.write "#{sLineEB2.gsub('<GUID>',guid).gsub('<subID>',subID)}\n"
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

#######################################################################################

desc 'Delete entitlements making backup first'
task :delete_batch, [:guid_position, :brand] do |task, args|
  puts "Batch deleting started"
  abort "Incorrect parameters: please use rake delete_batch[guid_position, brand] format" unless args[:guid_position] && args[:brand]
  csvFile = ENV['CSV']
  abort "File not found: #{csvFile}" unless csvFile && File.file?(csvFile)
  nCounter = 0
  now = Time.now.to_i
  # guid,brand,end_date,source,product,trace_id,start_date
  sFileName = "#{csvFile}_deleted_#{now}.csv"
  table_entitlements = "#{Cfg.config['cassandraCluster']['keyspace']}.#{Cfg.config['tables']['entitlements']}"
  @connection ||= Connection.instance.connection

  File.open(sFileName, "w") do |f|
    CSV.foreach(csvFile) do |row|
      if row[args[:guid_position].to_i] && row[args[:guid_position].to_i].strip.size>5 
        cql = "SELECT * FROM #{table_entitlements} WHERE guid='#{row[args[:guid_position].to_i]}' AND brand='#{args[:brand]}'"
        puts "Processing GUID: #{row[args[:guid_position].to_i]}, CQL: #{cql}"
        @connection.execute(cql).each do |ent_row|
          f.write "#{row[args[:guid_position].to_i]},#{args[:brand]},#{ent_row['end_date']},#{ent_row['source']},#{ent_row['product']},#{ent_row['trace_id']},#{ent_row['start_date']}\n"
          nCounter += 1
        end
        cql = "DELETE FROM #{table_entitlements} WHERE guid='#{row[args[:guid_position].to_i]}' AND brand='#{args[:brand]}'"
        puts "Deleting, CQL: #{cql}"
        @connection.execute(cql)
      else
        puts "Error processing GUID: #{row[args[:guid_position].to_i]}"
      end
    end
  end
  puts "#{nCounter} records deleted."
  puts "All the records are backuped at #{sFileName}"
end

#######################################################################################

desc 'Update entitlements'
task :update_batch do # accepts csv file with guid, old_trace_id, new_trace_id structure
  puts "Batch updatng started"
  csvFile = ENV['CSV']
  abort "File not found: #{csvFile}" unless csvFile && File.file?(csvFile)
  nCounter = 0

  table_entitlements = "#{Cfg.config['cassandraCluster']['keyspace']}.#{Cfg.config['tables']['entitlements']}"
  @connection ||= Connection.instance.connection

  CSV.foreach(csvFile) do |row|
    if row[0] && row[0].strip.size>5 
      cql = "SELECT * FROM #{table_entitlements} WHERE guid='#{row[0]}' AND brand='gcl'"
      puts "Processed #{nCounter}, now processing GUID: #{row[0]}"
      records = @connection.execute(cql)
      bProceed = false

      batch = @connection.batch do |batch|
        records.each do |ent_row|
          if ent_row['trace_id']==row[2]
            cql = "DELETE FROM #{table_entitlements} WHERE guid=? AND brand=? AND source=? AND product=? AND trace_id=? AND end_date=?"
            args = Array[ent_row['guid'],ent_row['brand'],ent_row['source'],ent_row['product'],ent_row['trace_id'],ent_row['end_date']]
            batch.add(cql, arguments: args)
            cql = "INSERT INTO #{table_entitlements} (guid, brand, end_date, source, product, trace_id, start_date) VALUES (?,?,?,?,?,?,?)"
            args = Array[ent_row['guid'],ent_row['brand'],ent_row['end_date'],ent_row['source'],ent_row['product'],row[1],ent_row['start_date']]
            batch.add(cql, arguments: args)
            nCounter += 1
            bProceed = true
          end
        end
      end
      @connection.execute(batch) if bProceed
    end
  end
  puts "#{nCounter} records pseudo updated"
end

#######################################################################################

desc 'Check entitlements as a batch'
task :check_batch, [:guid_position, :start_line] do |task, args|
  csvFile = ENV['CSV']
  abort "File not found: #{csvFile}" unless csvFile && File.file?(csvFile)

  table_entitlements = "#{Cfg.config['cassandraCluster']['keyspace']}.#{Cfg.config['tables']['entitlements']}"
  @connection ||= Connection.instance.connection
  nProcessed = 0
  nEntitled = 0
  search_date = Time.now.to_i*1000
  puts "check_batch started. guid_position=#{args[:guid_position]}, start_line=#{args[:start_line]}, search_date: #{search_date}"
  CSV.foreach(csvFile) do |row|
    bEntitled = false
    nProcessed += 1
    next if (nProcessed<=args[:start_line].to_i)
    cql = "SELECT toUnixTimestamp(start_date) AS start_date, toUnixTimestamp(end_date) AS end_date FROM #{table_entitlements} WHERE guid='#{row[args[:guid_position].to_i]}' AND brand='gcl'"
    @connection.execute(cql).each do |ent_row|
      bEntitled = true if ent_row['start_date']<search_date && ent_row['end_date']>search_date
    end
    puts "Entitlement found for line #{nProcessed}: #{row[args[:guid_position].to_i]}" if bEntitled
    nEntitled += 1 if bEntitled
  end
  puts "check_batch finished, #{nProcessed} records processed, #{nEntitled} entitlements found"
end

#######################################################################################

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
        url = Cfg.config['campAPI']['url'] + row[0].strip
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
        f.write "#{row[0].strip}," + response.to_s.gsub("\n",'')[39..-1] + "\n"
      else
        f.write "#{row[0].strip},SPDR response\n"
      end
      nCounter += 1
    end
  end
end

#######################################################################################

desc 'SPDR single call'
task :spdr_call do
  guid = ENV['GUID']
  abort "Incorrect guid: #{guid}" unless guid

  pem = File.read(Cfg.config['campAPI']['pemFile'])
  key = ENV['CAMP_KEY']

  uri = URI.parse(Cfg.config['campAPI']['url']+guid)
  puts "Trying URI: #{uri}"
  http = Net::HTTP.new(uri.host, uri.port)
  http.use_ssl = true
  http.ciphers = 'DEFAULT:!DH'
  http.cert = OpenSSL::X509::Certificate.new(pem)
  http.key = OpenSSL::PKey::RSA.new(key)
  http.verify_mode = OpenSSL::SSL::VERIFY_NONE
  puts http.request(Net::HTTP::Get.new(uri.request_uri)).body
end

#######################################################################################

desc 'Compare GUIDs in from 2 CSV files'
task :guid_compare, [:guid1, :guid2] do |task, args|
  # format: rake guid_compare[4,4]
  csvFile1 = ENV['CSV1']
  csvFile2 = ENV['CSV2']
  abort "File not found: #{csvFile1}" unless csvFile1 && File.file?(csvFile1)
  abort "File not found: #{csvFile2}" unless csvFile2 && File.file?(csvFile2)
  raGuids = []
  guid_pos1 = args[:guid1].to_i
  guid_pos2 = args[:guid2].to_i

  nCounter = 0
  CSV.foreach(csvFile1) do |row|
#    puts "Reading line #{nCounter} for file 1"
    raGuids << row[guid_pos1].strip.downcase
    nCounter += 1
  end
  puts "raGuids initialized with #{nCounter} records"

  nCounter = 0
  CSV.foreach(csvFile2) do |row|
#    puts "Checking line #{nCounter} at file 2"
    guid2 = row[guid_pos2].strip.downcase
    puts "#{guid2} exists at file 2 but not at file 1" unless raGuids.include? guid2
    nCounter += 1
  end

  puts "guid_compare task finished ok, #{nCounter} different records found"
end

#######################################################################################

desc 'Backup'
task :backup do
  sFileName = Time.now.strftime(Cfg.config['s3']['backupFileFormat'])

#  sKeyspace = "#{Cfg.config['cassandraCluster']['keyspace']}."
  sTableName = Cfg.config['tables']['entitlements']
  @connection ||= Connection.instance.connection
  nCounter = 0
  puts "Backup rake task started with fileName=#{sFileName}, tableName=#{Cfg.config['tables']['entitlements']}"

  File.open(sFileName, "w") do |f|
    f.write "guid,brand,end_date,source,product,trace_id,start_date\n"
    result  = @connection.execute("SELECT * FROM #{Cfg.config['cassandraCluster']['keyspace']}.#{Cfg.config['tables']['entitlements']}", page_size: 1000)
    loop do

      result.each do |row|
        f.write "#{row['guid']},#{row['brand']},#{row['end_date']},#{row['source']},#{row['product']},#{row['trace_id']},#{row['start_date']}\n"
        nCounter += 1
      end
      puts "nCounter=#{nCounter}"

      break if result.last_page?
      result = result.next_page
    end

  end
  puts "#{nCounter} records backuped at #{sFileName}"

  puts "Trying to upload #{sFileName} as #{File.basename(sFileName)} onto bucket #{Cfg.config['s3']['bucket']} for region: #{ENV['AWS_REGION']}"

  Aws.config.update({
    region: ENV['AWS_REGION'],
    credentials: Aws::Credentials.new(ENV['AWS_ACCESS_KEY_ID'], ENV['AWS_SECRET_ACCESS_KEY'])
  })

  S3 = Aws::S3::Resource.new(region: ENV['AWS_REGION'])
  bucket = S3.bucket(Cfg.config['s3']['bucket'])
  puts "bucket: #{bucket.inspect}"

  obj = bucket.object(sFileName)

  File.open(sFileName, 'rb') do |file|
    obj.put(body: file)
  end

  puts "Deleting file #{sFileName}"
  File.delete(sFileName)
  puts "Backup task finished ok"
end

#######################################################################################
=begin
desc 'Import records to db'
task :import, [:guid, :brand, :product, :source, :trace_id, :end_date, :trace_id_suffix] do |task, args|
  now = Time.now.to_i
  puts "Data import, task=#{task}, args=#{args}"
  abort "Incorrect parameters: please use rake import[:guid, :brand, :product, :source, :trace_id, :end_date, :trace_id_suffix] format" unless args[:trace_id_suffix]
  csvFile = ENV['IMPORT_CSV']
  abort "File not found: #{csvFile}" unless csvFile && File.file?(csvFile)
  table_entitlements = "#{Cfg.config['cassandraCluster']['keyspace']}.#{Cfg.config['tables']['entitlements']}"
  @connection ||= Connection.instance.connection
  nTransaction = 200
  nCounter = 0
  nTotal = 0

  CSV.foreach(csvFile) do |row|
    batch = @connection.batch do |batch|
      entitlements.each do |row|
        cql = "DELETE FROM #{@table_entitlements} WHERE guid=? AND brand=? AND source=? AND product=? AND trace_id=? AND end_date=?"
        args = Array[row['guid'],row['brand'],row['source'],row['product'],row['trace_id'],row['end_date']*1000]
        puts "Connection.moveEntitlementsToArchive, adding to batch: CQL=#{cql} with arguments: #{args}}\n"
        batch.add(cql, arguments: args)
        cql = "UPDATE #{@table_history} SET archive_type='#{msg}',start_date=? WHERE guid=? AND brand=? AND source=? AND product=? AND trace_id=? AND end_date=? AND archive_date=toTimestamp(NOW())"
        args = Array[row['start_date']*1000,row['guid'],row['brand'],row['source'],row['product'],row['trace_id'],row['end_date']*1000]
        puts "Connection.moveEntitlementsToArchive, adding to batch: CQL=#{cql} with arguments: #{args}}\n"
        batch.add(cql, arguments: args)
        result += 1
      end
    end
    @connection.execute(batch) if result>0


      billing = (args[:billing].to_i < 0 ) ? 'direct bill' : row[args[:billing].to_i]
      if billing && (billing.strip.downcase=='direct bill') then
        begin
          rateplan = (args[:rateplan].to_i < 0 ) ? '' : row[args[:rateplan].to_i].strip.downcase
          guid = row[args[:guid].to_i].strip
          subID = row[args[:subID].to_i].strip
          autorenew = (args[:autorenew].to_i < 0 ) ? 'true' : row[args[:autorenew].to_i].strip.downcase
          if (rateplan.include? 'french') then
            f.write "#{sLineFrench.gsub('<GUID>',guid).gsub('<subID>',subID)}\n"
          elsif ((rateplan.include? 'monthly') && (autorenew=='true')) then
            f.write "#{sLineMonthly.gsub('<GUID>',guid).gsub('<subID>',subID)}\n"
          elsif (autorenew=='true') then
            f.write "#{sLineEB1.gsub('<GUID>',guid).gsub('<subID>',subID)}\n"
            #f.write "#{sLineEB2.gsub('<GUID>',guid).gsub('<subID>',subID)}\n"
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

=end