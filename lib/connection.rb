require 'cassandra'
require 'singleton'
require './lib/cassandra_record.rb'

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

  def getEntitlements(guid, brand, source = '', product = '', trace_id = '', search_date = DateTime.now)
  	cql = "SELECT * FROM #{@table_entitlements} WHERE guid=? AND brand=? AND end_date>?"
  	args = [guid, brand, search_date]
    future = @connection.execute_async(cql, arguments: args)
    future.on_success do |rows|
      rows.each do |row|
        puts row['end_date']
      end
    end
    future.join  
  end
end