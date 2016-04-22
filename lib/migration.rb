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
    @connection.execute("use entitlements_dev") if use
    @connection.execute(cql)
  end
end