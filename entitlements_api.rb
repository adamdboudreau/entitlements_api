require 'grape'
require './entitlements'

module EntitlementsApi
  class API < Grape::API
    version 'v1', using: :header, vendor: 'entitlements_api'
    format :json

    resource :entitlements do

      desc 'return entitlement'
      params do
        requires :guid, type: String, desc: 'Guid'
      end
      route_param :guid do
        get do
          puts "get guid: #{params[:guid]}"
          e = Entitlements.find(guid: params[:guid]).first
          e.nil? ? {guid: 'guid not found'} : {guid: e.guid, type: e.type, brand: e.brand, product: e.product, start_date: e.start_date, end_date: e.end_date}
        end
      end

      desc 'Create an entitlement'
      params do
        requires :guid, type: String, desc: 'Guid for your entitlement'
      end
      post do
        e = Entitlements.new({guid: params[:guid], type: params[:type], brand: params[:brand], product: params[:product], start_date: params[:start_date], end_date: params[:end_date]})
        puts "e: #{e.inspect}"
        e.save
      end
    end
  end
end