require 'grape'
require './config/config.rb'
require './lib/cassandra_record.rb'
require './helpers/application_helper.rb'
require './models/request_heartbeat.rb'
require './models/request_entitled.rb'
require './models/entitlement.rb'

module EntitlementsService
  class API < Grape::API
    version 'v1', using: :path, vendor: 'dtc_entitlements_service'
    format :json

    resource :heartbeat do
      get do
        RequestHeartbeat.new(params).process
      end
    end

    resource :entitled do
      get do
        RequestEntitled.new(params).process
      end
    end

    resource :entitlements do

      desc 'Create an entitlement'
      params do
        requires :guid, type: String, desc: 'Guid for your entitlement'
        requires :end_date, type: Date, desc: 'End date'
      end
      post do
        e = Entitlements.new({guid: params[:guid], type: params[:type], brand: params[:brand], product: params[:product], start_date: params[:start_date], end_date: params[:end_date]})
        $logger.debug "EntitlementsApi.create: #{e.inspect}"
        e.save
      end

      desc 'Get entitlements'
      params do
        requires :guid, type: String
        requires :brand, type: String
        optional :type, type: String
        optional :product, type: String
        optional :start_date, type: Date
        optional :end_date, type: Date
      end
      get ':guid' do
        e = Entitlements.find(guid: params[:guid]).first
        $logger.debug "EntitlementsApi.get: #{e.inspect}"
        e.nil? ? { success: false, message: 'Guid not found', guid: params[:guid]} : { success: true, record: e }
#          e.nil? ? { success: false, message: 'Guid not found', guid: params[:guid]} : {success: true, guid: e.guid, type: e.type, brand: e.brand, product: e.product, start_date: e.start_date, end_date: e.end_date}
      end

      desc 'Update entitlement'
      params do
        requires :guid, type: String, desc: 'Guid'
        requires :status, type: String, desc: 'Your status.'
      end
      put ':id' do
        authenticate!
        current_user.statuses.find(params[:id]).update({
          user: current_user,
          text: params[:status]
        })
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