require './lib/cassandra_record.rb'
require './lib/connection.rb'

class Migration
  def migrate
    up
  end

  def rollback
    down
  end

  def execute(cql, use = true)
    @connection ||= Connection.instance.connection
    @connection.execute("use #{Cfg.config['cassandraCluster']['keyspace']}") if use
    @connection.execute(cql)
  end
end