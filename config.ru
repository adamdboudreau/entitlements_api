require './config/config.rb'
require './lib/entitlements_service.rb'

$logger = Logger.new(Cfg.config['logFile'], 'daily')

run EntitlementsService::API