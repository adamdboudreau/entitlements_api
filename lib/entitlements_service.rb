require './config/config.rb'

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

    resource :cql do
      get do
        Request::CQL.new(headers, params).process
      end
    end

    resource :spdr do
      get do
        camp = CAMP.new
        params['guid'] + ((camp.check? params['guid']) ? ':true' : ':false')
      end
    end

  end
end