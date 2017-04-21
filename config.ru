require './config/config.rb'

puts "Resetting config parameters from admin"
EntitlementsService::Helper::applyAdminConfig EntitlementsService::Helper::getAdminResponse

run EntitlementsService::API