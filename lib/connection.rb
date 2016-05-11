class Connection
  include Singleton
  attr_reader :connection, :table_entitlements, :table_tc, :table_history

  def initialize
    @table_entitlements = Cfg.config['cassandraCluster']['keyspace'] + '.' + Cfg.config['tables']['entitlements']
    @table_tc = Cfg.config['cassandraCluster']['keyspace'] + '.' + Cfg.config['tables']['tc']
    @table_history = Cfg.config['cassandraCluster']['keyspace'] + '.' + Cfg.config['tables']['history_entitlements']

  	cluster = { hosts: Cfg.config['cassandraCluster']['hosts'], port: Cfg.config['cassandraCluster']['port'] }
  	cluster[:username] = Cfg.config['cassandraCluster']['username'] unless Cfg.config['cassandraCluster']['username'].empty?
  	cluster[:password] = Cfg.config['cassandraCluster']['password'] unless Cfg.config['cassandraCluster']['password'].empty?
    @connection = Cassandra.cluster(cluster).connect
  end

  def putEntitled (params)
    start_date = params['start_date'] ? params['start_date'].to_i : Time.now.getutc
    end_date = params['end_date'] ? params['end_date'].to_i : Time.utc(2099, 1, 1)
    cql = "UPDATE #{@table_entitlements} SET start_date=?, end_date=? WHERE guid=? AND brand=? AND product=? AND source=? AND trace_id=?"
    args = [start_date, end_date, params['guid'], params['brand'], params['product'], params['source'], params['trace_id']]
    $logger.debug "\nConnection.putEntitled, running CQL=#{cql} with args=#{args}\n"
    statement = @connection.prepare(cql)
    @connection.execute(statement, arguments: args)
    putTC(params) unless Request::TC.new.validate
    true
  end

  def getEntitled(params)
    search_date = (params['search_date'] || Time.now).to_i
    cql = "SELECT * FROM #{@table_entitlements} WHERE guid=? AND brand=? AND end_date>?"
    result = Array.new
    @connection.execute(cql, arguments: [params['guid'], params['brand'], search_date]).each do |row|
      result << Entitlement.new(row) unless ((search_date<row['start_date']) || (params['source'] || row['source']!=params['source']) || (params['product'] || row['product']!=params['product']) || (params['trace_id'] || row['trace_id']!=params['trace_id']))
    end
    result  
  end

  def getEntitlements(params)
    search_date = (params['search_date'] || Time.now).utc
    cql = "SELECT * FROM #{@table_entitlements} WHERE guid=? AND brand=? AND end_date>?"
    result = Array.new
    @connection.execute(cql, arguments: [params['guid'], params['brand'], search_date]).each do |row|
      result << row unless ((search_date<row['start_date']) || (params['source'] || row['source']!=params['source']) || (params['product'] || row['product']!=params['product']) || (params['trace_id'] || row['trace_id']!=params['trace_id']))
    end
    result  
  end

  def getTC(params)
    result = nil
    if params['guid'] && (Cfg.config['brands'].include? params['brand'])
      cql = "SELECT * FROM #{@table_tc} WHERE guid=? AND brand=? LIMIT 1"
      @connection.execute(cql, arguments: [params['guid'], params['brand']]).each do |row|
        result = { version: row['tc_version'], acceptance_date: row['tc_acceptance_date'] }
      end 
    end
    result  
  end

  def putTC(params)
    cql = "UPDATE #{@table_tc} SET tc_acceptance_date=toTimestamp(now()), tc_version=? WHERE guid=? AND brand=?"
    args = [params['tc_version'], params['guid'], params['brand']]
    $logger.debug "\nConnection.putTC, running CQL=#{cql} with args=#{args}\n"
    statement = @connection.prepare(cql)
    @connection.execute(statement, arguments: args)
    true
  end

end