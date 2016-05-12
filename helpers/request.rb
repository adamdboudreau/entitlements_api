module Request

#-----------------------------------------------------------------------------------------------------------

  class AbstractRequest

    def initialize (type, params = {}, httptype = :get)
      @start_time = Time.now.to_f
      @type = type
      @params = params
      @httptype = httptype
      @response = { success: true }
      @error_message = nil
    end

    def process

      tc = ((@httptype==:get) && (@type!=:heartbeat)) ? Connection.instance.getTC(@params) : nil
      @response[:tc] = tc if tc

      @response[:request] = {}
      @response[:request][:type] = "#{@httptype.upcase} #{@type}"
      @response[:request][:input] = {}
      @params.each do |k, v| # return passed values
        @response[:request][:input][k] = v
      end
      @response[:request][:processingTimeMs] = '%.03f' % ((Time.now.to_f - @start_time)*1000)
      $logger.debug "\nRequest: /#{@type}/?" + URI.encode(@params.map{|k,v| "#{k}=#{v}"}.join("&")) + "\nResponse: #{@response.to_s}\n\n"
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
      super :entitled, params, httptype
    end

    def validate
      return 'Incorrect brand' unless Cfg.config['brands'].include? @params['brand']
      return 'Incorrect guid' unless @params['guid']
      return 'Incorrect source' unless @params['source']
      return 'Incorrect product' unless @params['product']
      return 'Incorrect trace_id' unless @params['trace_id']
      return 'Incorrect search_date' if (@httptype==:get) && @params['search_date'] && (@params['search_date'].to_i.to_s!=@params['search_date'])
      return 'Incorrect start_date' if (@httptype==:put) && @params['start_date'] && (@params['start_date'].to_i.to_s!=@params['start_date'])
      return 'Incorrect end_date' if (@httptype==:put) && @params['end_date'] && (@params['end_date'].to_i.to_s!=@params['end_date'])
      return 'Incorrect tc_version' if (@httptype==:put) && @params['tc_version'] && (@params['tc_version'].to_f.to_s!=@params['tc_version'])
      nil
    end

    def process
      if @error_message = validate # validation failed
        @response = { success: false, message: @error_message }
      else
        if @httptype==:put
          @response = { success: false, message: @error_message } unless Connection.instance.putEntitled(@params)
        else
          entitled_dates = Connection.instance.getEntitled(@params)
          @response['entitled'] = !!entitled_dates
          @response['start_date'] = entitled_dates[:start_date] if entitled_dates[:start_date]
          @response['end_date'] = entitled_dates[:end_date] if entitled_dates[:end_date]
        end
      end
      super
    end
  end

#-----------------------------------------------------------------------------------------------------------

  class Entitlements < AbstractRequest

    def initialize (params, httptype = :get)
      super :entitlements, params, httptype
    end

    def validate
      return 'Incorrect brand' unless Cfg.config['brands'].include? @params['brand']
      return 'Incorrect guid' unless @params['guid']
      return 'Incorrect source' if (@httptype==:delete) && !@params['source']
      nil
    end

    def process
      if @error_message = validate # validation failed
        @response = { success: false, message: @error_message }
      else
        if @httptype==:delete
          @response['deleted'] = Connection.instance.deleteEntitlements(@params)
        else
          @response['entitlements'] = Connection.instance.getEntitlements(@params)
        end
      end
      super
    end

  end

#-----------------------------------------------------------------------------------------------------------

  class TC < AbstractRequest

    def initialize (params, httptype = :get)
      super :tc, params, httptype
    end

    def validate
      return 'Incorrect brand' unless Cfg.config['brands'].include? @params['brand']
      return 'Incorrect guid' unless @params['guid']
      return 'Incorrect tc_version' unless (@httptype!=:put) || (@params['tc_version'].to_f.to_s == @params['tc_version'])
      # check if the passed version newer than existing one
      tc = Connection.instance.getTC(@params)
      return 'Too old tc_version to renew' if tc && (tc[:version].to_f > @params['tc_version'].to_f)
      nil
    end

    def process
      if @error_message = validate # validation failed
        @response = { success: false, message: @error_message }
      elsif @httptype==:put
        @response = { success: false, message: @error_message } unless Connection.instance.putTC(@params)
      end
      super
    end

  end

#-----------------------------------------------------------------------------------------------------------

  class CQL < AbstractRequest

    def initialize (params, httptype = :get)
      super :cql, params, httptype
    end

    def process
      @response = { response: Connection.instance.runCQL(@params) }
      super
    end

  end

#-----------------------------------------------------------------------------------------------------------

end
