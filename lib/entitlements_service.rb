
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
      uri = URI(Cfg.config['urlAdmin'])
      res = Net::HTTP.start(uri.host, uri.port, use_ssl: true) do |http|
        req = Net::HTTP::Get.new(uri)
        req['Content-Type'] = 'application/json'
        req['Authorization'] = "Token token=#{ENV['ADMIN_API_KEY']}"
        puts "ApplicationHelper.getAdminResponse. Request:\n#{req.inspect}"
        http.request(req)
      end
      JSON.parse res.body
    end

    def self.applyAdminConfig params
      puts "Helper::applyAdminConfig started with #{params}"
    end

  end

end