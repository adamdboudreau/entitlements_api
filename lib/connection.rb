#require 'cequel'
require 'cassandra'
require 'singleton'
require './lib/cassandra_record.rb'

class Connection
  include Singleton
  attr_reader :connection

  def initialize
puts 'connection.rb'
  	cluster = { hosts: Cfg.config['cassandraCluster']['hosts'], port: Cfg.config['cassandraCluster']['port'] }
  	cluster[:username] = Cfg.config['cassandraCluster']['username'] unless Cfg.config['cassandraCluster']['username'].empty?
  	cluster[:password] = Cfg.config['cassandraCluster']['password'] unless Cfg.config['cassandraCluster']['password'].empty?
p cluster  	
    @connection = Cassandra.cluster(cluster).connect

  end
end