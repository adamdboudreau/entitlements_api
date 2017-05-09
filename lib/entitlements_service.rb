
module EntitlementsService
  class API < Grape::API
    version 'v1', using: :path, vendor: 'dtc_entitlements_service'
    format :json

    resource :heartbeat do
      get do
        Request::Heartbeat.new(headers, params).process
      end
    end

    resource :entitlement do
      put do
        Request::Entitlement.new(headers, params, :put).process
      end
    end

    resource :entitlements do
      get do
        Request::Entitlements.new(headers, params).process
      end
      delete do
        Request::Entitlements.new(headers, params, :delete).process
      end
    end

    resource :tc do
      get do
        Request::TC.new(headers, params).process
      end
      put do
        Request::TC.new(headers, params, :put).process
      end
    end

    resource :archive do
      get do
        Request::Archive.new(headers, params).process
      end
      post do
        Request::Archive.new(headers, params, :post).process
      end
    end

    resource :reset do
      post do
        Request::Reset.new(headers, params).process
      end
    end

    resource :cql do
      get do
        Request::CQL.new(headers, params).process
      end
    end

    route :any, '*path' do
      { "success"=>false, "error_code"=>4006, "message"=>"Unknown request" }
    end

  end

#######################################################################################################################

  class Helper

    def self.getAdminResponse
      begin
        uri = URI(Cfg.config['urlAdmin'])
        res = Net::HTTP.start(uri.host, uri.port, use_ssl: true) do |http|
          req = Net::HTTP::Get.new(uri)
          req['Content-Type'] = 'application/json'
          req['Authorization'] = "Token token=#{ENV['ADMIN_API_KEY']}"
          puts "Helper.getAdminResponse. Request:\n#{req.inspect}"
          http.request(req)
        end
        JSON.parse res.body
      rescue Exception => e
        puts "ERROR! Helper.getAdminResponse EXCEPTION: cannot connect to admin tool at #{Cfg.config['urlAdmin']}"
        puts "#{e.message}\nBacktrace: #{e.backtrace.inspect}"
        return {}
      end
    end

    def self.applyAdminConfig params
      puts "\nHelper::applyAdminConfig started with #{params}\n\n"
      Cfg.config['entitlementAddons'] = [] unless Cfg.config['entitlementAddons'] && Cfg.config['entitlementAddons'].kind_of?(Array)
      if params['entitlements'] && params['entitlements']['addon_entitlements'] && params['entitlements']['addon_entitlements'].kind_of?(Array)
        params['entitlements']['addon_entitlements'].each do |e|
          puts "Helper::applyAdminConfig processing addon entitlement #{e['entitlement_name']}"
          Cfg.config['entitlementAddons'].push(e['entitlement_name'])
        end
      end
      puts "\nHelper::applyAdminConfig importing api keys\n"
      begin
        if params['api_keys'] && params['api_keys'].kind_of?(Array)
          puts "\nHelper::applyAdminConfig importing api keys from #{params['api_keys']}\n"
          params['api_keys'].each do |k|
            puts "Helper.applyAdminParams adding api key #{k['access_token']} with access level #{k['access_level']}"
            Cfg.config['apiKeys'][k['access_token']] = (Cfg.config['apiKeyTemplates'][k['access_level'].to_s]).clone
            Cfg.config['apiKeys'][k['access_token']]['description'] = k['client_name']
          end
        else
          puts "Helper.applyAdminParams ERROR: api_keys parameter is not array: #{params['api_keys']}"
        end
      rescue Exception => e
        puts "ERROR! Helper.applyAdminParams EXCEPTION on applying keys: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
      end

    end

  end

end