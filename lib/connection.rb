require 'cassandra'
require 'singleton'

class Connection
  include Singleton
  attr_reader :connection

  def initialize
    @connection = Cassandra.cluster(
        # username: 'admin',
        # password: '',
        hosts: Cfg.cassandraHosts
    ).connect
  end
end