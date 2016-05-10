module Request

#-----------------------------------------------------------------------------------------------------------

  class AbstractRequest

    def initialize (type, params = {}, httptype = :get)
      @start_time = Time.now.to_f
      @type = type
      @params = params
      @httptype = httptype
      @response = { success: "true" }
    end

    def process
      @response[:tc] = tc if (@httptype==:get) && (tc = Connection.instance.getTC(@params))

      @response[:request] = {}
      @response[:request][:type] = "#{@httptype.upcase} #{@type}"
      @response[:request][:input] = {}
      @params.each do |k, v| # return passed values
        @response[:request][:input][k] = v
      end
      @response[:request][:processingTimeMs] = '%.03f' % ((Time.now.to_f - @start_time)*1000)
      $logger.debug "\nRequest: /#{@type}/?" + URI.encode(@params.map{|k,v| "#{k}=#{v}"}.join("&")) + "\nResponse: #{@response.to_s}"
      @response
    end

  end

#-----------------------------------------------------------------------------------------------------------

  class Heartbeat < AbstractRequest
    def initialize (params)
      super :heartbeat, params
    end
  end

#-----------------------------------------------------------------------------------------------------------

  class Entitled < AbstractRequest

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
        @response['entitled'] = (Connection.instance.getEntitlements(@params).length > 0).to_s
      else
        @response = { success: "false", message: validateMessage }
      end
      super
    end

  end

#-----------------------------------------------------------------------------------------------------------

  class TC < AbstractRequest

    def initialize (params, httptype = :get)
      @params = params
      super :tc, params, httptype
    end

    def validate
      return 'Incorrect brand' unless Cfg.config['brands'].include? @params['brand']
      return 'Incorrect guid' unless @params['guid']
      ''
    end

    def process
      unless '' == validateMessage = validate
        @response = { success: 'false', message: validateMessage }
      end
      super
    end

  end

#-----------------------------------------------------------------------------------------------------------

end
