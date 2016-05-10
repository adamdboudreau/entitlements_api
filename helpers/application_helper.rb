module ApplicationHelper

  def loadColumns(connection, keyspace_name, table_name)
  	cql = "select column_name,type,clustering_order from system_schema.columns where keyspace_name='#{keyspace_name}' and table_name='#{table_name}'"
    columns = connection.execute(cql).reduce({}) do |columns, row|
      columns[row['column_name'].to_sym] = Cassandra::Column.new(row['column_name'], row['type'], row['clustering_order'])
      columns
    end
    columns
  end

  module_function :loadColumns

end
