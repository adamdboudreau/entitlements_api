module Request

#-----------------------------------------------------------------------------------------------------------

  class AbstractRequest

    def initialize (type, headers, params = {}, httptype = :get, bypass_api_check = false)
      @start_time = Time.now.to_f
      @bypass_api_check = bypass_api_check
      @api_key = headers ? headers['Authorization'] : ''
      @type = type
      @httptype = httptype
      @response = { success: true }
      @error_code = 0
      @error_message = nil

      @params = params
      @params['brand'].downcase! if @params['brand']
      @params['source'].downcase! if @params['source']
      @params['product'].downcase! if @params['product']

      # for testing
#      Connection.instance.close if @params['disconnect']

      puts "AbstractRequest.initialize finished with\ntype=#{type}\nparams=#{@params.to_json}\nhttptype=#{httptype}\nbypass_api_check=#{bypass_api_check}"
      puts "AbstractRequest.initialize finished with API key=#{@api_key}\nAPI key description: " + ((Cfg.config['apiKeys'][@api_key] && Cfg.config['apiKeys'][@api_key]['description']) ? Cfg.config['apiKeys'][@api_key]['description'] : '')
    end

    def validate
      puts "AbstractRequest.validate started. httptype=#{@httptype}"
      begin @error_code = 4006; return "Incorrect http type: #{@httptype}" end unless Cfg.requestParameters[@httptype.to_s]
      begin @error_code = 4006; return "Incorrect request type: #{@type}" end unless Cfg.requestParameters[@httptype.to_s][@type.to_s]

      # check if any empty parameter passed
      @params.each do |param|
        begin @error_code = 4003; return "Incorrect parameter name: #{param[0]}" end unless Cfg.requestParameters[@httptype.to_s][@type.to_s][param[0]]
        begin @error_code = 4004; return "Incorrect parameter value: #{param[0]}" end unless param[1] && param[1].strip.length>0
      end

      unless @bypass_api_check
        begin @error_code = 4000; return 'Incorrect API key' end unless Cfg.config['apiKeys'][@api_key]
        begin @error_code = 4007; return 'Not authorized' end unless Cfg.config['apiKeys'][@api_key]['allowed'][@httptype.to_s] && Cfg.config['apiKeys'][@api_key]['allowed'][@httptype.to_s][@type.to_s]
        begin @error_code = 4008; return 'API key expired' end unless DateTime.parse(Cfg.config['apiKeys'][@api_key]['allowed'][@httptype.to_s][@type.to_s])>DateTime.now
      end

      return true if (@httptype==:get) && (@type==:heartbeat || @type==:cql)
      begin @error_code = 4001; return "No connection to Cassandra" end unless Connection.instance.connection
      return true if (@httptype==:post) && (@type==:archive)
      begin @error_code = 4004; return 'Missed guid' end unless @params['guid']
      @error_code = 4005
      return 'Incorrect brand' unless Cfg.config['brands'].include? @params['brand']
#      return 'Incorrect guid' unless Cfg.isGUIDValid?(@params['guid'])
# commented out for prod      @params['guid'].insert(20,'-').insert(16,'-').insert(12,'-').insert(8,'-') if Cfg.isHyphensInjectionRequired?(@params['guid'])
      return 'Incorrect search_date' if @params['search_date'] && (@params['search_date'].to_i.to_s != @params['search_date'])
      return 'Incorrect start_date' if @params['start_date'] && (@params['start_date'].to_i.to_s != @params['start_date'])
      return 'Incorrect end_date' if @params['end_date'] && (@params['end_date'].to_i.to_s != @params['end_date'])
      @error_code = 0

      true
    end

    def process
      begin
        tc = ((@error_code==0) && (@httptype==:get) && (@type!=:heartbeat)) ? Connection.instance.getTC(@params) : nil
        @response[:tc] = tc if tc
      rescue Exception => e
        puts "ERROR! AbstractRequest EXCEPTION with getTC: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
        @error_code = 5000 if @error_code==0
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
      @response[:error_code] = @error_code if @error_code>0
      @response[:api_version] = Cfg.version if @params['api_version']
      @response[:processing_time_ms] = '%.03f' % ((Time.now.to_f - @start_time)*1000)
      puts "Request: /#{@type}/?" + URI.encode(@params.map{|k,v| "#{k}=#{v}"}.join("&")) + "\nResponse: #{@response.to_s}\n"
#      Connection.instance.connect if @params['disconnect']
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
      begin @error_code = 4004; return 'Missed source' end if (@httptype==:put) && !@params['source']
      begin @error_code = 4004; return 'Missed product' end if (@httptype==:put) && !@params['product'] && !@params['products']
      begin @error_code = 4005; return 'Duplicated product/products' end if (@httptype==:put) && @params['product'] && @params['products']
      begin @error_code = 4005; return 'Incorrect product, use products instead' end if (@httptype==:put) && @params['product'] && (@params['product'].include? ",")
      begin @error_code = 4004; return 'Missed trace_id' end if (@httptype==:put) && !@params['trace_id']
      begin @error_code = 4005; return 'Incorrect tc_version' end if (@httptype==:put) && @params['tc_version'] && (@params['tc_version'].to_f.to_s!=@params['tc_version'])
      begin @error_code = 4007; return "API key does not have permission for this operation" end unless validateKeyLimitations
      true
    end

    def validateKeyLimitations
      return true unless (@httptype==:put) && Cfg.config['apiKeys'][@api_key]['limited'] && Cfg.config['apiKeys'][@api_key]['limited']['put'] && Cfg.config['apiKeys'][@api_key]['limited']['put']['entitlement']
      Cfg.config['apiKeys'][@api_key]['limited']['put']['entitlement'].each do |param|
        paramName = param[0]
        values = param[1]
        return false unless values.include? @params[paramName]
      end
      true
    end

    def process
      if (@error_message = validate) == true # validation ok

        if @httptype==:put
          begin
            # build an array of params for multiple products inserting
            raParams = []
            if @params['products']
              @params['products'].split(',').each do |product|
                raParams << @params.clone.except('products')
                raParams[-1]['product'] = product
              end
            else
              raParams = [@params]
            end
            @response['updated'] = !(@response['created'] = (Connection.instance.putEntitlement(raParams)==0))
          rescue Exception => e
            puts "ERROR! Entitlement EXCEPTION with putEntitlement: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
            @error_code = 5000
            @response = { success: false, message: 'Unknown error during creating/updating an entitlement' }
          end

        else
          @error_code = 4006
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
      begin @error_code = 4004; return 'Incorrect source' end if (@httptype==:delete) && !@params['source']
      begin @error_code = 4007; return "API key does not have permission for this operation" end unless validateKeyLimitations
      true
    end

    def validateKeyLimitations
      return true unless (@httptype==:delete) && Cfg.config['apiKeys'][@api_key]['limited'] && Cfg.config['apiKeys'][@api_key]['limited']['delete'] && Cfg.config['apiKeys'][@api_key]['limited']['delete']['entitlements']
      Cfg.config['apiKeys'][@api_key]['limited']['delete']['entitlements'].each do |param|
        paramName = param[0]
        values = param[1]
        if @params[paramName]
          if (paramName=='products')
            @params['products'].split(',').each do |product|
              return false unless values.include? product
            end
          else
            return false unless values.include? @params[paramName]
          end
        else
          @params[paramName] = values.join(',')
        end
      end
      true
    end

    def process
      if (@error_message = validate) == true # validation ok

        if @httptype==:delete
          begin
            @response['deleted'] = Connection.instance.deleteEntitlements(@params)
          rescue Exception => e
            puts "ERROR! Entitlements EXCEPTION with deleteEntitlements: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
            @error_code = 5000
            @response = { success: false, message: 'Unknown error during deleting' }
          end

        elsif @httptype==:get

          begin
            @response['entitlements'] = Connection.instance.getEntitlements(@params)
            @response['entitled'] = Cfg.containsBaseEntitlement(@response['entitlements'])
          rescue Exception => e
            puts "ERROR! Entitlements EXCEPTION with getEntitlements: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
            @error_code = 5000
            @response = { success: false, entitled: true, entitlements: [] }
          end

        else
          @error_code = 4006
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

    def initialize (headers, params, httptype = :get, bypass_api_check = false)
      super :tc, headers, params, httptype, bypass_api_check
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
          puts "ERROR! TC EXCEPTION with getEntitlements: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
          @error_code = 5000
          return 'Error getting TC'
        end
      end
      true
    end

    def process
      if (@error_message = validate) == true # validation ok
        begin @error_code = 5000; @response = { success: false, message: @error_message } end if @httptype==:put && !Connection.instance.putTC(@params)
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
          begin @error_code = 5000; @response = { success: false, message: 'Unknown error' } end if @response['processed'] < 0
        elsif @httptype == :get
          @response['entitlements'] = Connection.instance.getArchive(@params, 'start_date')
        else
          @error_code = 4006
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
