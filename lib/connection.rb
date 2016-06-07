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
    begin
      @connection = Cassandra.cluster(cluster).connect
    rescue Exception => e
      $logger.error "Connection.initialize EXCEPTION: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
    end
  end

  def close
    @connection = nil
  end

  def connect
    initialize
  end

  def putEntitlement (params, delete_existing = true) # return a number of deleted records, or raises an exception on error
    $logger.debug "\nConnection.putEntitlement started with params: #{params}\n"

    result = delete_existing ? self.deleteEntitlements(params, 'Updated') : 0
    $logger.debug "\nConnection.putEntitled, params['start_date']=#{params['start_date']}\n"
    start_date = params['start_date'] ? Time.at(params['start_date'].to_i) : Time.now
    end_date = params['end_date'] ? Time.at(params['end_date'].to_i) : Time.utc(2222, 1, 1)
    cql = "UPDATE #{@table_entitlements} SET start_date=? WHERE end_date=? AND guid=? AND brand=? AND product=? AND source=? AND trace_id=?"
    args = [start_date, end_date, params['guid'], params['brand'], params['product'], params['source'], params['trace_id']]
    $logger.debug "\nConnection.putEntitled, running CQL=#{cql} with args=#{args}\n"
    statement = @connection.prepare(cql)
    @connection.execute(statement, arguments: args)
    putTC(params) if Request::TC.new(params, :put).validate==true
    result
  end

  def getEntitlements(params, exclude_future_entitlements = true, check_spdr = true)
    $logger.debug "\nConnection.getEntitlements started with params: #{params}, exclude_future_entitlements=#{exclude_future_entitlements}, check_spdr=#{check_spdr}\n"
    result = Array.new
    products = params['products'] ? params['products'].split(',') : (params['product'] ? [params['product']] : nil)
    search_date = (params['search_date'] ? Time.at(params['search_date']) : Time.now).to_i*1000
    cql = "SELECT guid, brand, source, product, trace_id, toUnixTimestamp(start_date) AS start_date, toUnixTimestamp(end_date) AS end_date FROM #{@table_entitlements} WHERE guid=? AND brand=? AND end_date>?"
    args = [params['guid'], params['brand'], search_date]
    @connection.execute(cql, arguments: args).each do |row|
      unless ((exclude_future_entitlements && (search_date<row['start_date'])) || 
              (params['source'] && row['source']!=params['source']) || 
              (products && (!products.include? row['product'])) || 
              (params['trace_id'] && row['trace_id']!=params['trace_id'])
             )
        row['start_date'] = row['start_date']/1000
        row['end_date'] = row['end_date']/1000
        result << row 
      end
    end

    # ping SPDR if no entitlements found
    if (check_spdr && result.empty? && (CAMP.new.check? params['guid']))
      paramsToInsert = Hash[
        'guid'=>params['guid'], 
        'brand'=>params['brand'], 
        'product'=>'gcl', 
        'source'=>'spdr', 
        'trace_id'=>params['guid'],
        'start_date'=>Time.now.to_i.to_s,
        'end_date'=>(Time.now + 60*Cfg.config['campAPI']['defaultSpdrProvisioningMins']).to_i.to_s
      ]
      $logger.debug "\nConnection.getEntitlements, found entitlement at SPDR, inserting entitlement to Cassandra: #{paramsToInsert}\n"
      putEntitlement paramsToInsert, false
      return Connection.instance.getEntitlements(params, exclude_future_entitlements, false)
    end

    $logger.debug "\nConnection.getEntitlements, running CQL=#{cql} with args=#{args}, returning #{result.length} row(s)\n"
    result  
  end

  def deleteEntitlements(params, msg = 'Deleted')
    $logger.debug "\nConnection.deleteEntitlements started with params: #{params}, msg=#{msg}\n"
    moveEntitlementsToArchive getEntitlements(params, false), msg
  end

  def moveEntitlementsToArchive(entitlements, msg)
    $logger.debug "\nConnection.moveEntitlementsToArchive started with #{entitlements.length} records, msg=#{msg}\n"
    result = 0

    batch = @connection.batch do |batch|
      entitlements.each do |row|
        cql = "DELETE FROM #{@table_entitlements} WHERE guid=? AND brand=? AND source=? AND product=? AND trace_id=? AND end_date=?"
        args = Array[row['guid'],row['brand'],row['source'],row['product'],row['trace_id'],row['end_date']*1000]
        $logger.debug "Connection.moveEntitlementsToArchive, adding to batch: CQL=#{cql} with arguments: #{args}}\n"
        batch.add(cql, arguments: args)
        cql = "UPDATE #{@table_history} SET archive_type='#{msg}',start_date=? WHERE guid=? AND brand=? AND source=? AND product=? AND trace_id=? AND end_date=? AND archive_date=toTimestamp(NOW())"
        args = Array[row['start_date']*1000,row['guid'],row['brand'],row['source'],row['product'],row['trace_id'],row['end_date']*1000]
        $logger.debug "Connection.moveEntitlementsToArchive, adding to batch: CQL=#{cql} with arguments: #{args}}\n"
        batch.add(cql, arguments: args)
        result += 1
      end
    end
    @connection.execute(batch) if result>0
    result
  end

  def getTC(params)
    $logger.debug "\nConnection.getTC started with params: #{params}\n"
    result = nil
    if params['guid'] && (Cfg.config['brands'].include? params['brand'])
      cql = "SELECT tc_version, toUnixTimestamp(tc_acceptance_date) AS tc_acceptance_date FROM #{@table_tc} WHERE guid=? AND brand=? LIMIT 1"
      args = [params['guid'], params['brand']]
      $logger.debug "\nConnection.getTC, running CQL=#{cql} with args=#{args}\n"
      @connection.execute(cql, arguments: args).each do |row|
        result = { version: row['tc_version'], acceptance_date: row['tc_acceptance_date'].to_i/1000 }
      end 
    end
    result  
  end

  def putTC(params)
    $logger.debug "\nConnection.putTC started with params: #{params}\n"
    begin
      cql = "UPDATE #{@table_tc} SET tc_acceptance_date=toTimestamp(now()), tc_version=? WHERE guid=? AND brand=?"
      args = [params['tc_version'], params['guid'], params['brand']]
      $logger.debug "\nConnection.putTC, running CQL=#{cql} with args=#{args}\n"
      statement = @connection.prepare(cql)
      @connection.execute(statement, arguments: args)
      true
    rescue Exception => e
      $logger.error "Connection.putTC EXCEPTION: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
      false
    end
   end

  def getArchive(params, orderBy = 'start_date')
    $logger.debug "\nConnection.getArchive started with params: #{params}\n"
    result = Array.new
    begin
      cql = "SELECT source, product, trace_id, toUnixTimestamp(start_date) AS start_date, toUnixTimestamp(end_date) AS end_date, toUnixTimestamp(archive_date) AS archive_date, archive_type FROM #{@table_history} WHERE guid=? AND brand=?"
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
      $logger.error "Connection.getArchive EXCEPTION: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
    end  
    $logger.debug "\nConnection.getArchive, running CQL=#{cql} with args=#{args}, returning #{result.length} row(s)\n"
    result.sort! { |a,b| a[orderBy] <=> b[orderBy] }
  end

  def postArchive
    $logger.debug "\nConnection.postArchive started\n"
    cql = "SELECT guid, brand, source, product, trace_id, toUnixTimestamp(start_date) AS start_date, toUnixTimestamp(end_date) AS end_date FROM #{@table_entitlements_by_enddate} WHERE end_date<toTimestamp(NOW()) LIMIT #{Cfg.config['archiveLimitPerRun']} ALLOW FILTERING"
    entitlements = Array.new
    begin
      @connection.execute(cql).each do |row|
        row['start_date'] = row['start_date']/1000
        row['end_date'] = row['end_date']/1000
        entitlements << row 
      end
      moveEntitlementsToArchive entitlements, 'Cleanup'
    rescue Exception => e
      $logger.error "Connection.postArchive EXCEPTION: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
      -1
    end  
  end

  def runCQL(params)
    $logger.debug "\nConnection.runCQL started with params: #{params}\n"
  	result = Array.new
  	begin
      @connection.execute(params['q']).each do |row|
        result << row
      end 
      $logger.debug "Connection.runCQL finished ok, result=#{result.to_s}"
    rescue Exception => e
      $logger.error "Connection.runCQL EXCEPTION: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
    end
    result
  end
end
