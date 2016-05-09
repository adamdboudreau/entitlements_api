require './models/request.rb'

class RequestEntitled < Request

  def initialize (params, httptype = :get)
  	@params = params
    super :entitled, params, httptype
  end

  def validate
  	return 'Incorrect brand' unless Cfg.config['brands'].include? @params['brand']
    return 'Incorrect guid' unless @params['guid']
    return 'Incorrect source' unless @params['source']
    return 'Incorrect product' unless @params['product']
    return 'Incorrect trace_id' unless @params['trace_id']
    ''
  end

  def process
    if '' == validateMessage = validate
      @response['entitled'] = Connection.instance.getEntitlements('', '')
    else
      @response = { success: "false", message: validateMessage }
    end
    super
  end

end