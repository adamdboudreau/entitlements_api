class Connection
  include Singleton
  attr_reader :connection, :table_entitlements, :table_tc, :table_history

  def initialize
    @table_entitlements = "#{Cfg.config['cassandraCluster']['keyspace']}.#{Cfg.config['tables']['entitlements']}"
    @table_entitlements_by_enddate = "#{Cfg.config['cassandraCluster']['keyspace']}.#{Cfg.config['tables']['entitlements_by_enddate']}"
    @table_tc = "#{Cfg.config['cassandraCluster']['keyspace']}.#{Cfg.config['tables']['tc']}"
    @table_history = "#{Cfg.config['cassandraCluster']['keyspace']}.#{Cfg.config['tables']['history_entitlements']}"

    cluster = { hosts: Cfg.config['cassandraCluster']['hosts'], port: Cfg.config['cassandraCluster']['port'] }
    cluster[:username] = Cfg.config['cassandraCluster']['username'] unless Cfg.config['cassandraCluster']['username'].empty?
    cluster[:password] = Cfg.config['cassandraCluster']['password'] unless Cfg.config['cassandraCluster']['password'].empty?
    puts "Connection.initialize started"
    begin
      if Cfg.config['cassandraCluster']['use_ssl']
        cluster[:server_cert] = Cfg.config['cassandraCluster']['certServer']
        cluster[:client_cert] = Cfg.config['cassandraCluster']['certClient']
        cluster[:private_key] = Cfg.config['cassandraCluster']['certKey']
        cluster[:passphrase] = ENV['CASSANDRA_PASSPHRASE']
      end
      @connection = Cassandra.cluster(cluster).connect
      puts "Connection.initialize: connected to Cassandra OK!"
    rescue Exception => e
      puts "ERROR! Connection.initialize EXCEPTION: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
    end
  end

  def close
    @connection = nil
  end

  def connect
    initialize
  end

  def putEntitlement (params_array, delete_existing = true) # returns a number of deleted records, or raises an exception on error
    puts "Connection.putEntitlement started with params_array: #{params_array}\n"
    result = 0

    params_array.each do |params|
      puts "Connection.putEntitlement, iteration started, params['start_date']=#{params['start_date']}\n"
      result = delete_existing ? self.deleteEntitlements(params, 'Updated') : 0
      start_date = params['start_date'] ? Time.at(params['start_date'].to_i) : Time.now
      end_date = params['end_date'] ? Time.at(params['end_date'].to_i) : Time.utc(2222, 1, 1)
      cql = "UPDATE #{@table_entitlements} SET start_date=? WHERE end_date=? AND guid=? AND brand=? AND product=? AND source=? AND trace_id=?"
      args = [start_date, end_date, params['guid'], params['brand'], params['product'], params['source'], params['trace_id']]
      puts "Connection.putEntitlement, running CQL=#{cql} with args=#{args}\n"
      statement = @connection.prepare(cql)
      @connection.execute(statement, arguments: args)
      params.except!("source","product","trace_id","start_date","end_date")
      putTC(params) if Request::TC.new(nil, params, :put, true).validate==true
    end
    result
  end

  def getEntitlements(params, exclude_future_entitlements = true, check_spdr = true)
    puts "Connection.getEntitlements started with params: #{params}, exclude_future_entitlements=#{exclude_future_entitlements}, check_spdr=#{check_spdr}\n"
    result = Array.new
    products = params['products'] ? params['products'].split(',') : params['product']
    sources = params['source'] ? params['source'].split(',') : nil
    trace_ids = params['trace_id'] ? params['trace_id'].split(',') : nil
    search_date = (params['search_date'] ? Time.at(params['search_date'].to_i) : Time.now).to_i*1000
    exclude_future_entitlements = exclude_future_entitlements && params.key?('search_date')
    cql = "SELECT guid, brand, source, product, trace_id, toUnixTimestamp(start_date) AS start_date, toUnixTimestamp(end_date) AS end_date FROM #{@table_entitlements} WHERE guid=? AND brand=? AND end_date>?"
    args = [params['guid'], params['brand'], search_date]
    @connection.execute(cql, arguments: args).each do |row|
      unless ((exclude_future_entitlements && (search_date<row['start_date'])) || 
              (products && (!products.include? row['product'])) || 
              (sources && (!sources.include? row['source'])) || 
              (trace_ids && (!trace_ids.include? row['trace_id']))
             )
        row['start_date'] = row['start_date']/1000
        row['end_date'] = row['end_date']/1000
        result << row 
      end
    end

# comment out to disable SPDR checking
    # ping SPDR if no entitlements found, or if the only entitlement is addon (for example, gameplus)
    if (check_spdr && !Cfg.containsBaseEntitlement(result))
      moveEntitlementsToArchive(result, "Deleted before SPDR check") if result.count>0
      puts 'Connection.getEntitlements, checking entitlements at SPDR to insert them to Cassandra'
      putEntitlement CAMP.new.getEntitlementParamsToInsert(params), false
      return Connection.instance.getEntitlements(params, exclude_future_entitlements, false)
    end

    puts "Connection.getEntitlements, running CQL=#{cql} with args=#{args}, returning #{result.length} row(s)\n"
    result  
  end

  def deleteEntitlements(params, msg = 'Deleted')
    puts "Connection.deleteEntitlements started with params: #{params}, msg=#{msg}\n"
    moveEntitlementsToArchive getEntitlements(params, false, false), msg
  end

  def moveEntitlementsToArchive(entitlements, msg)
    puts "Connection.moveEntitlementsToArchive started with #{entitlements.length} records, msg=#{msg}\n"
    result = 0

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
    result
  end

  def getTC(params)
    puts "Connection.getTC started with params: #{params}\n"
    result = nil
    return nil unless params['guid'] && (!params['guid'].strip.empty?) && (Cfg.config['brands'].include? params['brand'])

    cql = "SELECT tc_version, toUnixTimestamp(tc_acceptance_date) AS tc_acceptance_date FROM #{@table_tc} WHERE guid=? AND brand=? LIMIT 1"
    args = [params['guid'], params['brand']]
    puts "Connection.getTC, running CQL=#{cql} with args=#{args}\n"
    @connection.execute(cql, arguments: args).each do |row|
      result = { version: row['tc_version'], acceptance_date: row['tc_acceptance_date'].to_i/1000 }
    end
    result  
  end

  def putTC(params)
    puts "Connection.putTC started with params: #{params}\n"
    begin
      cql = "UPDATE #{@table_tc} SET tc_acceptance_date=toTimestamp(now()), tc_version=? WHERE guid=? AND brand=?"
      args = [params['tc_version'], params['guid'], params['brand']]
      puts "Connection.putTC, running CQL=#{cql} with args=#{args}\n"
      statement = @connection.prepare(cql)
      @connection.execute(statement, arguments: args)
      true
    rescue Exception => e
      puts "ERROR!! Connection.putTC EXCEPTION: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
      false
    end
   end

  def getArchive(params, orderBy = 'start_date')
    puts "Connection.getArchive started with params: #{params}\n"
    result = Array.new
    begin
      cql = "SELECT brand, guid, source, product, trace_id, toUnixTimestamp(start_date) AS start_date, toUnixTimestamp(end_date) AS end_date, toUnixTimestamp(archive_date) AS archive_date, archive_type FROM #{@table_history} WHERE guid=? AND brand=?"
      args = [params['guid'], params['brand']]
      @connection.execute(cql, arguments: args).each do |row|
        unless ((params['source'] && row['source']!=params['source']) || 
                (params['product'] && row['product']!=params['product']) || 
                (params['trace_id'] && row['trace_id']!=params['trace_id'])
               )
          row['start_date'] = row['start_date']/1000
          row['end_date'] = row['end_date']/1000
          row['archive_date'] = row['archive_date']/1000
          result << row 
        end
      end
    rescue Exception => e
      puts "ERROR!! Connection.getArchive EXCEPTION: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
    end  
    puts "Connection.getArchive, running CQL=#{cql} with args=#{args}, returning #{result.length} row(s)\n"
    result.sort! { |a,b| a[orderBy] <=> b[orderBy] }
  end

  def postArchive (limit = nil)
    limit ||= Cfg.config['archiveLimitPerRun']
    cql = "SELECT guid, brand, source, product, trace_id, toUnixTimestamp(start_date) AS start_date, toUnixTimestamp(end_date) AS end_date FROM #{@table_entitlements_by_enddate} WHERE end_date<toTimestamp(NOW()) LIMIT #{limit} ALLOW FILTERING"
    puts "Connection.postArchive started, CQL=#{cql}"
    entitlements = Array.new
    begin
      @connection.execute(cql).each do |row|
        row['start_date'] = row['start_date']/1000
        row['end_date'] = row['end_date']/1000
        entitlements << row 
      end
      puts "Connection.postArchive entitlements to archive: #{entitlements}"
      moveEntitlementsToArchive entitlements, 'Cleanup'
    rescue Exception => e
      puts "ERROR!! Connection.postArchive EXCEPTION: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
      -1
    end  
  end

  def runCQL(params)
    puts "Connection.runCQL started with params: #{params}\n"
  	result = Array.new
  	begin
      @connection.execute(params['q']).each do |row|
        result << row
      end 
      puts "Connection.runCQL finished ok, result=#{result.to_s}"
    rescue Exception => e
      puts "ERROR!! Connection.runCQL EXCEPTION: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
    end
    result
  end

  def deleteZuora
    puts "\nConnection.deleteZuora started\n"
    cql = "SELECT * FROM #{@table_entitlements}"
    nDeleted = 0
    begin
      rows = @connection.execute(cql)
      puts "Going to process #{rows.length} records\n"
      rows.each do |row|
        if ((row['source'] == 'zuora') && (row['guid'] == row['trace_id']))
          cql = "DELETE FROM #{@table_entitlements} WHERE guid=? AND end_date=? AND brand=? AND product=? AND source='zuora' AND trace_id=?"
          args = [row['guid'], row['end_date'], row['brand'], row['product'], row['trace_id']]
          puts "Deleting record #{nDeleted} for guid=#{row['guid']}\n"
          @connection.execute(@connection.prepare(cql), arguments: args)
          nDeleted += 1
        end
      end
      nDeleted
    rescue Exception => e
      puts "ERROR! Connection.deleteZuora EXCEPTION: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
      -1
    end  
  end

end
