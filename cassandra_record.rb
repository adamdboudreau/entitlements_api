require './connection.rb'
# load 'connection.rb'
require './string.rb'

class CassandraRecord
  attr_accessor :new_record

  def initialize(params = {})
    params.each do |k,v|
      self.send("#{k}=",v)
    end
  end

  def new_record?
    new_record
  end

  class << self
    attr_reader :columns, :table_name, :connection, :update_sql, :keys
  end
  CassandraColumn = Struct.new(:name, :type, :data_type)

  def self.inherited(sub_class)
    sc = sub_class.to_s
    if i = sc.rindex('::')
      sc = sc[(i+2)..-1]
    end
    table_name = sc.to_s.underscore
    connection = Connection.instance.connection
    columns = load_columns(connection, table_name)

    sub_class.instance_variable_set :@table_name, table_name
    sub_class.instance_variable_set :@connection, connection
    sub_class.instance_variable_set :@columns, columns
    stm = connection.prepare "INSERT INTO #{table_name} (#{columns.keys.join(',')}) VALUES (#{columns.keys.map{'?'}.join(',')})"
    sub_class.instance_variable_set :@update_sql, stm
    sub_class.instance_variable_set :@keys, columns.select{|_,c| c.type == 'partition_key' || c.type == 'clustering_key'}.values

    columns.each do |column, _|
      sub_class.send(:attr_accessor, column)
    end
  end

  def self.load_columns(connection, table_name)
    connection.execute('use system')
    columns = connection.execute("select column_name,type, validator from schema_columns where keyspace_name='entitlements_dev' and columnfamily_name='#{table_name}'")
                  .reduce({}) do |columns, row|
      columns[row['column_name'].to_sym] = CassandraColumn.new(
          row['column_name'], row['type'], row['validator']
      )
      columns
    end
    connection.execute("use entitlements_dev")
    columns
  end

  def save(options = {})
    params = self.class.columns.map do |c|
      send(c[0]) || case c[1].data_type
                      when /ListType/
                        []
                      when /(Int32Type|LongType)/
                        0
                    end
    end
    self.class.connection.execute(self.class.update_sql, arguments: params, hints: options[:hints])
  end

  def self.all
    find
  end

  def self.delete(options = {})
    sql = "DELETE FROM #{@table_name} "
    sql += ' WHERE '
    sql += options.map{|key, _| "#{key} = ?" }.join(' AND ')

    statement = connection.prepare(sql)
    connection.execute(statement, arguments: options.values)
  end

  def delete
    sql = "DELETE FROM #{self.class.table_name} WHERE "
    condition = []
    condition_values = []
    self.class.keys.each do |key|
      unless (val = send(key.name)).nil?
        condition << " #{key.name} = ? "
        condition_values << val
      end
    end
    sql += condition.join(' AND ')
    self.class.connection.execute(sql, arguments: condition_values)
  end

  def self.find(options = {})
    sql = "select * from #{@table_name} "
    if options.size > 0
      sql += ' where '
      sql += options.map{|key, val| "#{key} #{val.kind_of?(Array) ? 'in' : '='} ?" }.join(' AND ')
    end
    statement = connection.prepare(sql)
    rows = connection.execute(statement, arguments: options.values)
    rows.reduce([]) do |res, row|
      obj = new(options)
      obj.include_concern!
      row.each { |c, v| obj.public_send(c + '=', v) }
      res.push obj
    end
  end

  def self.find_or_initialize(options = {})
    object = find(options).first || options.each_with_object(new(options)) {|(c, v), obj| obj.public_send(c.to_s + '=', v) }
    object.include_concern!
  end

  def include_concern!
    if self.respond_to?(:league) && !self.league.blank?
      concern_class = "::#{self.class.to_s.gsub(/^Models::/, 'Concerns::')}::#{self.league}"
      self.send :extend, Util.const_get(concern_class) if Util.const_defined? concern_class
    end
    self
  end

  def self.first(options = {})
    find(options).first
  end
end