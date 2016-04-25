require 'cassandra'
require 'singleton'

class Connection
  include Singleton
  attr_reader :connection

  def initialize
    @connection = Cassandra.cluster(Cfg.config['cassandraCluster']).connect
  end
end