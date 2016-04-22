require './data/config.rb'
require './lib/entitlements_service.rb'

$logger = Logger.new('logs/entitlements.log', 'daily')

run EntitlementsService::API