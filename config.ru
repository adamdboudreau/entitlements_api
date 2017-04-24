require './config/config.rb'

puts "Importing config parameters from admin"
EntitlementsService::Helper::applyAdminConfig EntitlementsService::Helper::getAdminResponse

run EntitlementsService::API