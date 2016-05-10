require 'cassandra'
require 'singleton'
#require './lib/cassandra_record.rb'

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

  def getEntitlements(params)
    search_date = (params['search_date'] || Time.now).to_i
    cql = "SELECT * FROM #{@table_entitlements} WHERE guid=? AND brand=? AND end_date>?"
    result = Array.new
    @connection.execute(cql, arguments: [params['guid'], params['brand'], search_date]).each do |row|
      result << Entitlement.new(row) unless ((search_date<row['start_date']) || (params['source'] || row['source']!=params['source']) || (params['product'] || row['product']!=params['product']) || (params['trace_id'] || row['trace_id']!=params['trace_id']))
    end
    result  
  end

  def getTC(params)
    result = nil
    if params['guid'] && (Cfg.config['brands'].include? params['brand'])
      cql = "SELECT * FROM #{@table_tc} WHERE guid=? AND brand=? LIMIT 1"
      @connection.execute(cql, arguments: [params['guid'], params['brand']]).each do |row|
        result = TC.new(row) 
      end 
    end
    result  
  end

end