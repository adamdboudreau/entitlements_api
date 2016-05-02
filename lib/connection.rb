require 'cassandra'
require 'singleton'
require './lib/cassandra_record.rb'

class Connection
  include Singleton
  attr_reader :connection

  def initialize
  	cluster = { hosts: Cfg.config['cassandraCluster']['hosts'], port: Cfg.config['cassandraCluster']['port'] }
  	cluster[:username] = Cfg.config['cassandraCluster']['username'] unless Cfg.config['cassandraCluster']['username'].empty?
  	cluster[:password] = Cfg.config['cassandraCluster']['password'] unless Cfg.config['cassandraCluster']['password'].empty?
    @connection = Cassandra.cluster(cluster).connect
  end
end