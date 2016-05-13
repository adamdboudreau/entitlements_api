require './config/config.rb'

$logger = Logger.new(Cfg.config['logFile'], 'daily')

run EntitlementsService::API