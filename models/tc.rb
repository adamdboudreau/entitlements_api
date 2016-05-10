class TC

  attr_accessor :values

  def initialize (row)
    @values = row
  end
  
  def store(params)
  	return false unless validate(params)
    if params['guid'] && (Cfg.config['brands'].include? params['brand'])
      cql = "SELECT * FROM #{@table_tc} WHERE guid=? AND brand=? LIMIT 1"
      @connection.execute(cql, arguments: [params['guid'], params['brand']]).each do |row|
        result = TC.new(row) 
      end 
    end
  end

  def validate(params)
  end

end