require 'cassandra'
require 'singleton'

class Connection
  include Singleton
  attr_reader :connection

  def initialize
    @connection = Cassandra.cluster(
        # username: 'admin',
        # password: '',
        hosts: ['127.0.0.1']
    ).connect
  end
end