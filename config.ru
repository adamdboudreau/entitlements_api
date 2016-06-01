require './config/config.rb'

#$logger = Logger.new(Cfg.config['logFile'], 'daily')
$logger = Le.new('entitlements', :local => Cfg.config['logFile'])

run EntitlementsService::API