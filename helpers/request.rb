module Request

#-----------------------------------------------------------------------------------------------------------

  class AbstractRequest

    def initialize (type, headers, params = {}, httptype = :get)
      @start_time = Time.now.to_f
      @type = type
      @params = params
      @httptype = httptype
      @response = { success: true }
      @error_message = nil
      @api_key = headers['Authorization']
      # for testing
      Connection.instance.close if @params['disconnect']
    end

    def validate
      $logger.debug "\nAbstractRequest.validate started\n"
      return 'Incorrect API key' unless Cfg.config['apiKeys'][@api_key]
      return 'Not authorized' unless Cfg.config['apiKeys'][@api_key]['allowed'][@httptype.to_s] && Cfg.config['apiKeys'][@api_key]['allowed'][@httptype.to_s][@type.to_s]
      return 'API key expired' unless DateTime.parse(Cfg.config['apiKeys'][@api_key]['allowed'][@httptype.to_s][@type.to_s])>DateTime.now
      return true if (@httptype==:get) && (@type==:heartbeat || @type==:cql)
      return true if (@httptype==:put) && (@type==:archive)
      return 'Incorrect brand' unless Cfg.config['brands'].include? @params['brand']
      return 'Incorrect guid' unless @params['guid']
      return 'Incorrect start_date' if @params['start_date'] && (@params['start_date'].to_i.to_s != @params['start_date'])
      return 'Incorrect end_date' if @params['end_date'] && (@params['end_date'].to_i.to_s != @params['end_date'])
      true
    end

    def process
      tc = ((@httptype==:get) && (@type!=:heartbeat)) ? Connection.instance.getTC(@params) : nil
      @response[:tc] = tc if tc

      if @params['echo']
        @response[:request] = {}
        @response[:request][:type] = "#{@httptype.upcase} #{@type}"
        @response[:request][:input] = {}
        @params.each do |k, v| # return passed values
          @response[:request][:input][k] = v
        end
      end
      @response[:processingTimeMs] = '%.03f' % ((Time.now.to_f - @start_time)*1000)
      $logger.debug "\nRequest: /#{@type}/?" + URI.encode(@params.map{|k,v| "#{k}=#{v}"}.join("&")) + "\nResponse: #{@response.to_s}\n\n"
      Connection.instance.connect if @params['disconnect']
      @response
    end

  end

#-----------------------------------------------------------------------------------------------------------

  class Heartbeat < AbstractRequest
    def initialize (headers, params)
      super :heartbeat, headers, params
    end

    def process
#      camp = CAMP.new
#      puts camp.check? 'ceeda127-8035-4f24-890e-2e6fbe4eb49b'
      @response = { success: false, message: @error_message } unless (@error_message = validate) == true
      super
    end
  end

#-----------------------------------------------------------------------------------------------------------

  class Entitled < AbstractRequest

    def initialize (headers, params, httptype = :get)
      super :entitled, headers, params, httptype
    end

    def validate
      $logger.debug "\nEntitled.validate started\n"
      return @error_message unless (@error_message = super) == true
      return 'Incorrect source' unless @params['source']
      return 'Incorrect product' unless @params['product']
      return 'Incorrect trace_id' unless @params['trace_id']
      return 'Incorrect search_date' if (@httptype==:get) && @params['search_date'] && (@params['search_date'].to_i.to_s!=@params['search_date'])
      return 'Incorrect start_date' if (@httptype==:put) && @params['start_date'] && (@params['start_date'].to_i.to_s!=@params['start_date'])
      return 'Incorrect end_date' if (@httptype==:put) && @params['end_date'] && (@params['end_date'].to_i.to_s!=@params['end_date'])
      return 'Incorrect tc_version' if (@httptype==:put) && @params['tc_version'] && (@params['tc_version'].to_f.to_s!=@params['tc_version'])
      $logger.debug "\nEntitled.validate finished ok\n"
      true
    end

    def process
      $logger.debug "\nEntitled.process started\n"
      if (@error_message = validate) == true # validation ok
        if @httptype==:put
          nDeleted = Connection.instance.putEntitled(@params)
          @response['updated'] = !(@response['created'] = (nDeleted==0))
          @response = { success: false, message: 'Unknown error' } if nDeleted<0 
        else
          entitled = Connection.instance.getEntitled(@params)
          @response = { success: entitled[:success], entitled: entitled[:entitled] }
          @response['start_date'] = entitled[:start_date] if entitled[:start_date]
          @response['end_date'] = entitled[:end_date] if entitled[:end_date]
        end
      else # validation failed
        @response = { success: false, message: @error_message }
      end
      super
    end
  end

#-----------------------------------------------------------------------------------------------------------

  class Entitlements < AbstractRequest

    def initialize (headers, params, httptype = :get)
      super :entitlements, headers, params, httptype
    end

    def validate
      return @error_message unless (@error_message = super) == true
      return 'Incorrect source' if (@httptype==:delete) && !@params['source']
      true
    end

    def process
      if (@error_message = validate) == true # validation ok
        if @httptype==:delete
          @response['deleted'] = Connection.instance.deleteEntitlements(@params)
          @response = { success: false, message: 'Unknown error during deleting' } if @response['deleted']<0
        else
          @response['entitlements'] = Connection.instance.getEntitlements(@params)
        end
      else # validation failed
        @response = { success: false, message: @error_message }
      end
      super
    end

  end

#-----------------------------------------------------------------------------------------------------------

  class TC < AbstractRequest

    def initialize (headers, params, httptype = :get)
      super :tc, headers, params, httptype
    end

    def validate
      # check if the passed version newer than existing one
      return @error_message unless (@error_message = super) == true
      if @httptype==:put
        return 'Incorrect tc_version' unless @params['tc_version'].to_f.to_s == @params['tc_version']
        tc = Connection.instance.getTC(@params)
        return 'Too old tc_version to renew' if tc && (tc[:version].to_f > @params['tc_version'].to_f)
      end
      true
    end

    def process
      if (@error_message = validate) == true # validation ok
        @response = { success: false, message: @error_message } if @httptype==:put && !Connection.instance.putTC(@params)
      else
        @response = { success: false, message: @error_message }
      end
      super
    end
  end

#-----------------------------------------------------------------------------------------------------------

  class Archive < AbstractRequest

    def initialize (headers, params, httptype = :get)
      super :archive, headers, params, httptype
    end

    def process
      if (@error_message = validate) == true
        if @httptype == :post
          @response['processed'] = Connection.instance.postArchive
          @response = { success: false, message: 'Unknown error' } if @response['processed'] < 0
        else
          @response['entitlements'] = Connection.instance.getArchive(@params)
        end
      else
        @response = { success: false, message: @error_message }
      end
      super
    end
  end

#-----------------------------------------------------------------------------------------------------------

  class CQL < AbstractRequest

    def initialize (headers, params, httptype = :get)
      super :cql, headers, params, httptype
    end

    def process
      @response = ((@error_message = validate) == true) ? { response: Connection.instance.runCQL(@params) } : { success: false, message: @error_message }
      super
    end

  end

#-----------------------------------------------------------------------------------------------------------

end
