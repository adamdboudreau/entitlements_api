require './cassandra_record.rb'

class Entitlements < CassandraRecord
  attr_accessor :guid, :type, :brand, :product, :start_date, :end_date
  
end