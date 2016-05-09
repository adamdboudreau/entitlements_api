require './lib/connection.rb'

class Entitlement

  attr_accessor :columns

  def initialize
  	@columns = ApplicationHelper.loadColumns(Connection.instance.connection, Cfg.config['cassandraCluster']['keyspace'], Cfg.config['tables']['entitlements'])
  end
  
  def store
  end
end