class Connection
  include Singleton
  attr_reader :connection, :table_entitlements, :table_tc, :table_history

  def initialize
    @table_entitlements = Cfg.config['cassandraCluster']['keyspace'] + '.' + Cfg.config['tables']['entitlements']
    @table_entitlements_by_enddate = Cfg.config['cassandraCluster']['keyspace'] + '.' + Cfg.config['tables']['entitlements_by_enddate']
    @table_tc = Cfg.config['cassandraCluster']['keyspace'] + '.' + Cfg.config['tables']['tc']
    @table_history = Cfg.config['cassandraCluster']['keyspace'] + '.' + Cfg.config['tables']['history_entitlements']

  	cluster = { hosts: Cfg.config['cassandraCluster']['hosts'], port: Cfg.config['cassandraCluster']['port'] }
  	cluster[:username] = Cfg.config['cassandraCluster']['username'] unless Cfg.config['cassandraCluster']['username'].empty?
  	cluster[:password] = Cfg.config['cassandraCluster']['password'] unless Cfg.config['cassandraCluster']['password'].empty?
    @connection = Cassandra.cluster(cluster).connect
  end

  def putEntitled (params)
    start_date = params['start_date'] ? Time.at(params['start_date'].to_i) : Time.now
    end_date = params['end_date'] ? params['end_date'].to_i : Time.utc(2222, 1, 1)
    cql = "UPDATE #{@table_entitlements} SET start_date=? WHERE end_date=? AND guid=? AND brand=? AND product=? AND source=? AND trace_id=?"
    args = [start_date, end_date, params['guid'], params['brand'], params['product'], params['source'], params['trace_id']]
    $logger.debug "\nConnection.putEntitled, running CQL=#{cql} with args=#{args}\n"
    statement = @connection.prepare(cql)
    @connection.execute(statement, arguments: args)
    putTC(params) unless Request::TC.new(params).validate
    true
  end

  def getEntitled(params)
    search_date = params['search_date'] ? Time.at(params['search_date'].to_i) : Time.now
    cql = "SELECT * FROM #{@table_entitlements_by_enddate} WHERE guid=? AND brand=? AND source=? AND product=? AND trace_id=? AND end_date>?"
    args = [params['guid'], params['brand'], params['source'], params['product'], params['trace_id'], search_date]
    $logger.debug "\nConnection.getEntitled, running CQL=#{cql} with args=#{args}\n"
    result = {}
    @connection.execute(cql, arguments: args).each do |row|
      result = { start_date: row['start_date'], end_date: row['end_date'] } if search_date>row['start_date']
    end
    $logger.debug "\nConnection.getEntitled, running CQL=#{cql} with args=#{args} returns #{result.to_s}\n"
    result  
  end

  def getEntitlements(params, exclude_future_entitlements = true)
    result = Array.new
    begin
      search_date = params['search_date'] ? Time.at(params['search_date'].to_i) : Time.now
      cql = "SELECT source, product, trace_id, start_date, end_date FROM #{@table_entitlements} WHERE guid=? AND brand=? AND end_date>?"
      args = [params['guid'], params['brand'], search_date]
      @connection.execute(cql, arguments: args).each do |row|
        result << row unless ((exclude_future_entitlements && (search_date<row['start_date'])) || 
                              (params['source'] && row['source']!=params['source']) || 
                              (params['product'] && row['product']!=params['product']) || 
        	                    (params['trace_id'] && row['trace_id']!=params['trace_id'])
        	                   )
      end
    rescue Exception => e
      $logger.error "Connection.deleteEntitlements EXCEPTION: #{e.message}"
      $logger.error "Connection.deleteEntitlements backtrace: #{e.backtrace.inspect}"
    end  
    $logger.debug "\nConnection.getEntitlements, running CQL=#{cql} with args=#{args}, returning #{result.length} row(s)\n"
    result  
  end

  def deleteEntitlements(params)
    result = 0
    begin
      batch = @connection.batch do |batch|
        getEntitlements(params, false).each do |row|
          cql1 = "DELETE FROM #{@table_entitlements} WHERE guid=? AND brand=? AND source=? AND product=? AND trace_id=? AND end_date=?"
          cql2 = "INSERT INTO #{@table_history} (guid,brand,source,product,trace_id,end_date,start_date,archive_date,archive_type) VALUES (?,?,?,?,?,?,?,NOW(),'Deleted')"
          args = Array[params['guid'],params['brand'],row['source'],row['product'],row['trace_id'],row['end_date']]
          $logger.debug "Connection.deleteEntitlements, adding to batch: CQL=#{cql1} with arguments: #{args}}\n"
          batch.add(cql1,args)
          $logger.debug "Connection.deleteEntitlements, adding to batch: CQL=#{cql2} with arguments: #{args<<row['start_date']}}\n"
          batch.add(cql2, args)
          result += 1
        end
      end
      @connection.execute(batch)
    rescue Exception => e
      $logger.error "Connection.deleteEntitlements EXCEPTION: #{e.message}"
      $logger.error "Connection.deleteEntitlements backtrace: #{e.backtrace.inspect}"
    end  
    result
  end

  def archiveEntitlements()
    result = 0
    cql = "SELECT * FROM #{@table_entitlements_by_enddate} WHERE end_date<toTimestamp(NOW()) ALLOW FILTERING"
    begin
      batch = @connection.batch do |batch|
        @connection.execute(cql).each do |row|
          args = Array[row['guid'],row['brand'],row['source'],row['product'],row['trace_id'],row['end_date']]
          batch.add("DELETE FROM #{@table_entitlements} WHERE guid=? AND brand=? AND source=? AND product=? AND trace_id=? AND end_date=?",args)
          batch.add("INSERT INTO #{@table_history} (guid,brand,source,product,trace_id,end_date,start_date,archive_date,archive_type) VALUES (?,?,?,?,?,?,?,NOW(),'Cleanup')", args<<row['start_date'])
          result += 1
        end
      end
      @connection.execute(batch)
      $logger.debug "\nConnection.archiveEntitlements, running CQL=#{cql}, returns #{result.to_s}\n"
    rescue Exception => e
      $logger.error "Connection.archiveEntitlements EXCEPTION: #{e.message}"
      $logger.error "Connection.archiveEntitlements backtrace: #{e.backtrace.inspect}"
    end  
    result
  end

  def getTC(params)
    result = nil
    if params['guid'] && (Cfg.config['brands'].include? params['brand'])
      begin
        cql = "SELECT * FROM #{@table_tc} WHERE guid=? AND brand=? LIMIT 1"
        args = [params['guid'], params['brand']]
        $logger.debug "\nConnection.getTC, running CQL=#{cql} with args=#{args}\n"
        @connection.execute(cql, arguments: args).each do |row|
          result = { version: row['tc_version'], acceptance_date: row['tc_acceptance_date'] }
        end 
      rescue Exception => e
        $logger.error "Connection.getTC EXCEPTION: #{e.message}"
        $logger.error "Connection.getTC backtrace: #{e.backtrace.inspect}"
      end  
    end
    result  
  end

  def putTC(params)
    begin
      cql = "UPDATE #{@table_tc} SET tc_acceptance_date=toTimestamp(now()), tc_version=? WHERE guid=? AND brand=?"
      args = [params['tc_version'], params['guid'], params['brand']]
      $logger.debug "\nConnection.putTC, running CQL=#{cql} with args=#{args}\n"
      statement = @connection.prepare(cql)
      @connection.execute(statement, arguments: args)
      true
    rescue Exception => e  
      $logger.error "Connection.putTC EXCEPTION: #{e.message}"
      $logger.error "Connection.putTC backtrace: #{e.backtrace.inspect}"
      false
    end  
   end

  def runCQL(params)
  	result = Array.new
  	begin
      $logger.debug "\nConnection.runCQL, running CQL=#{params['q']}\n"
      @connection.execute(params['q']).each do |row|
        result << row
      end 
      $logger.debug "Connection.runCQL finished ok, result=#{result.to_s}"
    rescue Exception => e  
      $logger.error "Connection.runCQL EXCEPTION: #{e.message}"
      $logger.error "Connection.runCQL backtrace: #{e.backtrace.inspect}"
    end  
    result
  end
end