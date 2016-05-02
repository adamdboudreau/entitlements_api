require 'cequel'

class Entitlement
  include Cequel::Record

  key :id, :int
  column :guid, :text
  column :type, :text
end