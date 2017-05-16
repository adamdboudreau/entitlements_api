require 'csv'
require 'aws-sdk'
require 'zip'
require 'net/sftp'

require './config/config.rb'
require './lib/migration.rb'

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

desc 'Select and save records that match regex'
task :select_regex, [:field, :regex] do |task, args|
  # example: rake select_regex[guid,'[0-9a-fA-F]{32}']
  table_entitlements = "#{Cfg.config['cassandraCluster']['keyspace']}.#{Cfg.config['tables']['entitlements']}"
  puts "select_regex rake task started, running SELECT * FROM #{table_entitlements}"
  @connection ||= Connection.instance.connection
  nProcessed = nPassed = 0
  now = Time.now.to_i
  sFileName = "selected_#{now}.csv"
  records = @connection.execute("SELECT * FROM #{table_entitlements}", page_size: 10000)

  File.open(sFileName, "w") do |f|
    loop do
      records.each do |row|
        if (/#{args[:regex]}/.match(row[args[:field]]).to_s==row[args[:field]])
          nProcessed += 1
          f.write "#{row['guid']},#{row['brand']},#{row['end_date']},#{row['source']},#{row['product']},#{row['trace_id']},#{row['start_date']}\n"
        else
          nPassed +=1
        end
      end
      puts "#{nPassed+nProcessed} processed\n"
      break if records.last_page?
      records = records.next_page
    end
  end
  puts "select_regex procedure finished for #{Time.now.to_i-now} seconds: #{nProcessed} records selected, #{nPassed} records passed"
end

#######################################################################################

desc 'Select and save records that require hyphen injection'
task :select_for_hyphen_injection do
  # example: rake select_regex[guid,'[0-9a-fA-F]{32}']
  table_entitlements = "#{Cfg.config['cassandraCluster']['keyspace']}.#{Cfg.config['tables']['entitlements']}"
  puts "select_for_hyphen_injection rake task started, running SELECT * FROM #{table_entitlements}"
  @connection ||= Connection.instance.connection
  nProcessed = nPassed = 0
  now = Time.now.to_i
  sFileName = "selected_#{now}.csv"
  records = @connection.execute("SELECT * FROM #{table_entitlements}", page_size: 10000)

  File.open(sFileName, "w") do |f|
    loop do
      records.each do |row|
        if Cfg.isHyphensInjectionRequired?(row['guid'])
          nProcessed += 1
          f.write "#{row['guid']},#{row['brand']},#{row['end_date']},#{row['source']},#{row['product']},#{row['trace_id']},#{row['start_date']}\n"
        else
          nPassed +=1
        end
      end
      puts "#{nPassed+nProcessed} processed\n"
      break if records.last_page?
      records = records.next_page
    end
  end
  puts "select_for_hyphen_injection procedure finished for #{Time.now.to_i-now} seconds: #{nProcessed} records selected, #{nPassed} records passed"
end

#######################################################################################

desc 'Process csv file inserting hyphens'
task :inject_hyphens do
  puts "inject_hyphens rake task started"
  csvFile = ENV['CSV']
  abort "File not found: #{csvFile}" unless csvFile && File.file?(csvFile)
  nCounter = 0
  now = Time.now.to_i
  # guid,brand,end_date,source,product,trace_id,start_date
  sNewFileName = "#{csvFile}-inserted.csv"

  File.open(sNewFileName, "w") do |f|
    CSV.foreach(csvFile) do |row|
      puts "Processing line #{nCounter}: #{row}"
      row[0].insert(20,'-').insert(16,'-').insert(12,'-').insert(8,'-')
      f.write "#{row[0]},#{row[1]},#{row[2]},#{row[3]},#{row[4]},#{row[5]},#{row[6]}\n"
      nCounter += 1
    end
  end
  puts "#{nCounter} records imported into #{sNewFileName} for #{Time.now.to_i-now} seconds"
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

desc 'Get entitlements as a batch'
task :get_batch, [:brand] do |task, args|
  puts "get_batch started"
  puts "pricing-test:test:google-ad".to_s.match(/:test/)   #would match , or "pricing-test:test:sn-banner"
  abort "Incorrect parameters: please use rake get_batch[brand] format" unless args[:brand]
  csvFile = ENV['CSV']
  abort "File not found: #{csvFile}" unless csvFile && File.file?(csvFile)
  nCounter = 0
  now = Time.now.to_i
  # guid,brand,end_date,source,product,trace_id,start_date
  sFileName = "#{csvFile}_#{now}.csv"
  table_entitlements = "#{Cfg.config['cassandraCluster']['keyspace']}.#{Cfg.config['tables']['entitlements']}"
  @connection ||= Connection.instance.connection

  File.open(sFileName, "w") do |f|
    CSV.foreach(csvFile) do |row|
      puts "Processing GUID: #{row[0]}"
      if row[0] && row[0].strip.size>5 
        row[0].insert(20,'-').insert(16,'-').insert(12,'-').insert(8,'-') if Cfg.isHyphensInjectionRequired?(row[0])
        puts "Processed GUID: #{row[0]}"
        cql = "SELECT * FROM #{table_entitlements} WHERE guid='#{row[0]}' AND brand='#{args[:brand]}'"
        puts "Running CQL: #{cql}"
        @connection.execute(cql).each do |ent_row|
          f.write "#{row[0]},#{args[:brand]},#{ent_row['end_date']},#{ent_row['source']},#{ent_row['product']},#{ent_row['trace_id']},#{ent_row['start_date']}\n"
          nCounter += 1
        end
      else
        puts "Error processing GUID: #{row[0]}"
      end
    end
  end
  puts "#{nCounter} records found."
  puts "All the records are stored at #{sFileName}"
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
        puts "Got from SPDR: #{response}"
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

desc 'Export'
task :export, [:table_name, :condition] do |task, args|
  sTableName = Cfg.config['cassandraCluster']['keyspace'] + '.' + args[:table_name]
  sFileName = "export_#{sTableName}_" + Time.now.to_i.to_s + '.csv'
  @connection ||= Connection.instance.connection
  nCounter = 0
  puts "Export rake task started with fileName=#{sFileName}, tableName=#{sTableName}"

  File.open(sFileName, "w") do |f|
    f.write "guid,brand,source,product,trace_id,start_date,end_date\n"
    result  = @connection.execute("SELECT * FROM #{sTableName} WHERE #{args[:condition]}", page_size: 1000)
    loop do
      result.each do |row|
        f.write "#{row['guid']},#{row['brand']},#{row['source']},#{row['product']},#{row['trace_id']},#{row['start_date']},#{row['end_date']}\n" if row['source'] == 'spdr'
        nCounter += 1
      end
      puts "Processed #{nCounter} records"

      break if result.last_page?
      result = result.next_page
    end
  end
  puts "#{nCounter} records backuped at #{sFileName}"
end

#######################################################################################

desc 'Backup'
task :backup, [:table_name] do |task, args|
  sTableName = args[:table_name] || Cfg.config['tables']['entitlements']
  sFileName = Time.now.strftime(Cfg.config['s3']['backupFileFormat'].sub('%%table%%',sTableName))
  sZipFileName = "#{sFileName}.zip"
  sTableName = Cfg.config['cassandraCluster']['keyspace'] + '.' + sTableName
  @connection ||= Connection.instance.connection
  nCounter = 0
  puts "Backup rake task started with fileName=#{sFileName}, tableName=#{sTableName}"

  File.open(sFileName, "w") do |f|
    f.write (args[:table_name] ? "guid,brand,tc_acceptance_date,tc_version\n" : "guid,brand,end_date,source,product,trace_id,start_date\n")
    result  = @connection.execute("SELECT * FROM #{sTableName}", page_size: 1000)
    loop do
      result.each do |row|
        if args[:table_name]
          f.write "#{row['guid']},#{row['brand']},#{row['tc_acceptance_date']},#{row['tc_version']}\n"
        else
          f.write "#{row['guid']},#{row['brand']},#{row['end_date']},#{row['source']},#{row['product']},#{row['trace_id']},#{row['start_date']}\n"
        end
        nCounter += 1
      end
      puts "Processed #{nCounter} records"

      break if result.last_page?
      result = result.next_page
    end
  end
  puts "#{nCounter} records backuped at #{sFileName}, start zipping"

  Zip::File.open(sZipFileName, Zip::File::CREATE) do |zipfile|
    zipfile.add(sFileName, sFileName)
  end
  puts "#{sFileName} zipped to #{sZipFileName}"

  puts "Trying to upload #{sZipFileName} as #{File.basename(sZipFileName)} onto bucket #{Cfg.config['s3']['bucket']} for region: #{ENV['AWS_REGION']}"

  Aws.config.update({
    region: ENV['AWS_REGION'],
    credentials: Aws::Credentials.new(ENV['AWS_ACCESS_KEY_ID'], ENV['AWS_SECRET_ACCESS_KEY'])
  })

  S3 = Aws::S3::Resource.new(region: ENV['AWS_REGION'])
  bucket = S3.bucket(Cfg.config['s3']['bucket'])
  puts "bucket: #{bucket.inspect}"

  obj = bucket.object(sZipFileName)

  File.open(sZipFileName, 'rb') do |file|
    obj.put(body: file)
  end

  puts "Deleting files #{sFileName} and #{sZipFileName}"
  File.delete(sZipFileName)
  File.delete(sFileName)
  puts "Backup task finished ok"
end

#######################################################################################

desc 'Backup delta'  # makes backup for the changes since the last backup
task :backupDelta, [:table_name] do |task, args|
  sTableName = args[:table_name] || Cfg.config['tables']['entitlements']
  sFileName = Time.now.strftime(Cfg.config['sftp']['backupFileFormat'].sub('%%table%%',sTableName))
  sZipFileName = "#{sFileName}.zip"
  sTableName = Cfg.config['cassandraCluster']['keyspace'] + '.' + sTableName
  @connection ||= Connection.instance.connection
  nTotal = 0
  nFound = 0
  start_date = (Time.now-24.hours).to_i*1000
  puts "BackupDelta rake task started with fileName=#{sFileName}, tableName=#{sTableName}, start_date=#{start_date}"

  File.open(sFileName, "w") do |f|
    result = nil
    if args[:table_name]=='tc'
      f.write ("guid,brand,tc_acceptance_date,tc_version\n")
      result = @connection.execute("SELECT guid,brand,toUnixTimestamp(tc_acceptance_date) AS tc_acceptance_date,tc_version FROM #{sTableName}", page_size: 1000)
    else
      f.write ("guid,brand,source,product,trace_id,start_date,end_date\n")
      result = @connection.execute("SELECT guid,brand,source,product,trace_id,toUnixTimestamp(start_date) AS start_date,end_date FROM #{sTableName}", page_size: 1000)
    end

    loop do
      result.each do |row|
        nTotal += 1
        if args[:table_name]=='tc' && row['tc_acceptance_date']>start_date
          f.write "#{row['guid']},#{row['brand']},#{row['tc_acceptance_date']},#{row['tc_version']}\n"
          nFound += 1
        elsif row['start_date']>start_date
          f.write "#{row['guid']},#{row['brand']},#{row['source']},#{row['product']},#{row['trace_id']},#{row['start_date']},#{row['end_date']}\n"
          nFound += 1
        end
      end
      puts "Found #{nFound} of #{nTotal} processed records"

      break if result.last_page?
      result = result.next_page
    end
  end
  puts "#{nFound}/#{nTotal} records backuped at #{sFileName}, start zipping"

  Zip::File.open(sZipFileName, Zip::File::CREATE) do |zipfile|
    zipfile.add(sFileName, sFileName)
  end
  puts "#{sFileName} zipped to #{sZipFileName}"

  puts "Trying to upload #{sZipFileName} as #{File.basename(sZipFileName)} onto sftp #{Cfg.config['sftp']['ip']}/#{Cfg.config['sftp']['directory']}"

  sftp = Net::SFTP.start(Cfg.config['sftp']['ip'], Cfg.config['sftp']['user'], keys: [Cfg.config['sftp']['certificate']], passphrase: ENV['DTCES_SFTP_PASSPHRASE'] )
  sftp.upload!(sZipFileName, "/#{Cfg.config['sftp']['directory']}/#{sZipFileName}")

  puts "Deleting files #{sFileName} and #{sZipFileName}"
  File.delete(sZipFileName)
  File.delete(sFileName)
  puts "BackupDelta task finished ok"
end

#######################################################################################

desc 'Import records to db'
task :import do
  now = Time.now.to_i
  csvFile = ENV['IMPORT_CSV']
  abort "File not found: #{csvFile}" unless csvFile && File.file?(csvFile)
  nTotal = 0
  sTableName = Cfg.config['cassandraCluster']['keyspace']+'.'+Cfg.config['tables']['entitlements']
  @connection ||= Connection.instance.connection

  raRows = []

  CSV.foreach(csvFile) do |row|
    raRows << row
    if raRows.length>199
      batch = @connection.batch do |batch|
        raRows.each do |row|
          unless row[0]=='GUID' # make sure we don't process header
            row[0].insert(20,'-').insert(16,'-').insert(12,'-').insert(8,'-') if Cfg.isHyphensInjectionRequired?(row[0])
            args = Array[row[0],row[3]]
            batch.add("INSERT INTO #{sTableName} (guid, brand, end_date, source, product, trace_id, start_date) VALUES (?,'gcl',7952399888000,'crm','fullgcl',?,1484092800000)", 
              arguments: args)
            nTotal += 1
            batch.add("INSERT INTO #{sTableName} (guid, brand, end_date, source, product, trace_id, start_date) VALUES (?,'gcl',7952399888000,'crm','gameplus',?,1484092800000)", 
              arguments: args)
            nTotal += 1
          end
        end
      end
      @connection.execute(batch)
      raRows = []
      puts "Processed: #{nTotal}"
    end
  end
  if raRows.length>0
    batch = @connection.batch do |batch|
      raRows.each do |row|
        row[0].insert(20,'-').insert(16,'-').insert(12,'-').insert(8,'-') if Cfg.isHyphensInjectionRequired?(row[0])
        args = Array[row[0],row[3]]
        batch.add("INSERT INTO #{sTableName} (guid, brand, end_date, source, product, trace_id, start_date) VALUES (?,'gcl',7952399888000,'crm','fullgcl',?,1484092800000)", 
            arguments: args)
        nTotal += 1
        batch.add("INSERT INTO #{sTableName} (guid, brand, end_date, source, product, trace_id, start_date) VALUES (?,'gcl',7952399888000,'crm','gameplus',?,1484092800000)", 
            arguments: args)
        nTotal += 1
      end
    end
    @connection.execute(batch)
  end
  puts "Importing finished with #{nTotal} records inserted, processed by #{Time.now.to_i-now} seconds"
end

#######################################################################################

desc 'Import records to db with all columns specified'
task :importAllColumns do
  now = Time.now.to_i
  csvFile = ENV['CSV']
  abort "File not found: #{csvFile}" unless csvFile && File.file?(csvFile)
  nTotal = 0
  sTableName = Cfg.config['cassandraCluster']['keyspace']+'.'+Cfg.config['tables']['entitlements']
  @connection ||= Connection.instance.connection

  raRows = []

  CSV.foreach(csvFile) do |row|
    raRows << row
    if raRows.length>99
      batch = @connection.batch do |batch|
        raRows.each do |row|
          if row[0] && row[0]!='GUID' # make sure the line is not empty and we don't process header
            row[0].insert(20,'-').insert(16,'-').insert(12,'-').insert(8,'-') if Cfg.isHyphensInjectionRequired?(row[0])
            row[4] = Date.parse(row[4]).to_time.to_i * 1000
            row[5] = Date.parse(row[5]).to_time.to_i * 1000
            batch.add("INSERT INTO #{sTableName} (GUID,Source,Brand,Product,Start_date,End_date,Trace_id) VALUES (?,?,?,?,?,?,?)", 
              arguments: row)
            nTotal += 1
          end
        end
      end
      @connection.execute(batch)
      raRows = []
      puts "Processed: #{nTotal}"
    end
  end
  if raRows.length>0
    batch = @connection.batch do |batch|
      raRows.each do |row|
        if row[0] && row[0]!='GUID' # make sure the line is not empty and we don't process header
          row[0].insert(20,'-').insert(16,'-').insert(12,'-').insert(8,'-') if Cfg.isHyphensInjectionRequired?(row[0])
          row[4] = Date.parse(row[4]).to_time.to_i * 1000
          row[5] = Date.parse(row[5]).to_time.to_i * 1000
          batch.add("INSERT INTO #{sTableName} (GUID,Source,Brand,Product,Start_date,End_date,Trace_id) VALUES (?,?,?,?,?,?,?)", 
            arguments: row)
          nTotal += 1
        end
      end
    end
    @connection.execute(batch)
  end
  puts "Importing finished with #{nTotal} records inserted, processed by #{Time.now.to_i-now} seconds"
end
