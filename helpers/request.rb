module Request

#-----------------------------------------------------------------------------------------------------------

  class AbstractRequest

    def initialize (type, headers, params = {}, httptype = :get)
      @api_key = headers['Authorization']
      $logger.info "\nAbstractRequest.initialize started with\ntype=#{type}\nparams=#{params.to_json}\nhttptype=#{httptype}"
      $logger.info "\nAbstractRequest.initialize started with API key=#{headers['Authorization']}\nAPI key description: " + ((Cfg.config['apiKeys'][@api_key] && Cfg.config['apiKeys'][@api_key]['description']) ? Cfg.config['apiKeys'][@api_key]['description'] : '')
      @start_time = Time.now.to_f
      @type = type
      @params = params
      @httptype = httptype
      @response = { success: true }
      @error_message = nil
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
      return 'Incorrect search_date' if @params['search_date'] && (@params['search_date'].to_i.to_s != @params['search_date'])
      return 'Incorrect start_date' if @params['start_date'] && (@params['start_date'].to_i.to_s != @params['start_date'])
      return 'Incorrect end_date' if @params['end_date'] && (@params['end_date'].to_i.to_s != @params['end_date'])
      true
    end

    def process
      begin
        tc = ((@httptype==:get) && (@type!=:heartbeat)) ? Connection.instance.getTC(@params) : nil
        @response[:tc] = tc if tc
      rescue Exception => e
        $logger.error "AbstractRequest EXCEPTION with getTC: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
        @response[:success] = false
      end

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
      @response = { success: false, message: @error_message } unless (@error_message = validate) == true
      super
    end
  end

#-----------------------------------------------------------------------------------------------------------

  class Entitlement < AbstractRequest

    def initialize (headers, params, httptype = :get)
      super :entitlement, headers, params, httptype
    end

    def validate
      return @error_message unless (@error_message = super) == true
      return 'Incorrect source' if (@httptype==:put) && !@params['source']
      return 'Incorrect product' if (@httptype==:put) && !@params['product']
      return 'Incorrect trace_id' if (@httptype==:put) && !@params['trace_id']
      return 'Incorrect tc_version' if (@httptype==:put) && @params['tc_version'] && (@params['tc_version'].to_f.to_s!=@params['tc_version'])
      true
    end

    def process
      if (@error_message = validate) == true # validation ok

        if @httptype==:put
          begin
            @response['updated'] = !(@response['created'] = (Connection.instance.putEntitlement(@params)==0))
          rescue Exception => e
            $logger.error "Entitlement EXCEPTION with putEntitlement: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
            @response = { success: false, message: 'Unknown error during creating/updating an entitlement' }
          end

        else
          @response = { success: false, message: 'Incorrect request type' }
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
          begin
            @response['deleted'] = Connection.instance.deleteEntitlements(@params)
          rescue Exception => e
            $logger.error "Entitlements EXCEPTION with deleteEntitlements: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
            @response = { success: false, message: 'Unknown error during deleting' }
          end

        elsif @httptype==:get

          begin
            @response['entitlements'] = Connection.instance.getEntitlements(@params)
            @response['entitled'] = !@response['entitlements'].empty?
          rescue Exception => e
            $logger.error "Entitlements EXCEPTION with getEntitlements: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
            @response = { success: false, entitled: true, entitlements: [] }
          end

        else
          @response = { success: false, message: 'Incorrect request type' }
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
        begin
          tc = Connection.instance.getTC(@params)
          return 'Too old tc_version to renew' if tc && (tc[:version].to_f > @params['tc_version'].to_f)
        rescue Exception => e
          $logger.error "TC EXCEPTION with getEntitlements: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
          return 'Error getting TC'
        end
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
        elsif @httptype == :get
          @response['entitlements'] = Connection.instance.getArchive(@params, 'start_date')
        else
          @response = { success: false, message: 'Incorrect request type' }
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
