require './config/config.rb'

$logger = (Cfg.config[:env]=='dev') ? Logger.new(Cfg.config['logFile'], 'daily') : Le.new(Cfg.config['logEntriesToken'])

run EntitlementsService::API