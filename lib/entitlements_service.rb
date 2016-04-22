require 'grape'
require './data/config.rb'
require './lib/entitlements.rb'

module EntitlementsService
  class API < Grape::API
    version 'v1', using: :header, vendor: 'entitlements_api'
    format :json

    resource :entitlements do

      desc 'Create an entitlement'
      params do
        requires :guid, type: String, desc: 'Guid for your entitlement'
      end
      post do
        e = Entitlements.new({guid: params[:guid], type: params[:type], brand: params[:brand], product: params[:product], start_date: params[:start_date], end_date: params[:end_date]})
        $logger.debug "EntitlementsApi.create: #{e.inspect}"
        e.save
      end

      desc 'Get entitlement'
      params do
        requires :guid, type: String, desc: 'Guid'
      end
      route_param :guid do
        get do
#          puts "get guid: #{params[:guid]}"
          e = Entitlements.find(guid: params[:guid]).first
          $logger.debug "EntitlementsApi.get: #{e.inspect}"
          e.nil? ? { success: false, message: 'Guid not found', guid: params[:guid]} : {success: true, record: e}
#          e.nil? ? { success: false, message: 'Guid not found', guid: params[:guid]} : {success: true, guid: e.guid, type: e.type, brand: e.brand, product: e.product, start_date: e.start_date, end_date: e.end_date}
        end
      end

      desc 'Update entitlement'
      params do
        requires :guid, type: String, desc: 'Guid'
      end
      route_param :guid do
        post do
#          puts "get guid: #{params[:guid]}"
          $logger.debug "EntitlementsApi.update: #{params[:guid]}"
          e = Entitlements.find(guid: params[:guid]).first
          e.nil? ? {guid: 'guid not found'} : {guid: e.guid, type: e.type, brand: e.brand, product: e.product, start_date: e.start_date, end_date: e.end_date}
        end
      end

      desc 'Delete entitlement'
      params do
        requires :guid, type: String, desc: 'Guid'
      end
      route_param :guid do
        post do
          $logger.debug "EntitlementsApi.delete: #{params[:guid]}"
          e = Entitlements.find(guid: params[:guid]).first
          e.nil? ? {guid: 'guid not found'} : {guid: e.guid, type: e.type, brand: e.brand, product: e.product, start_date: e.start_date, end_date: e.end_date}
        end
      end

    end
  end
end