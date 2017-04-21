require 'cassandra'
require 'grape'
require 'json'
require 'singleton'
require 'net/smtp'
require 'net/http'
require 'uri'
require 'timeout'

require './lib/connection.rb'
require './lib/entitlements_service.rb'
require './helpers/request.rb'
require './helpers/camp.rb'

class Cfg
  include Singleton

  environments = Dir.glob('./config/*.json').select{ |f| File.file? f }.map { |f| File.basename(f, '.*' ) }
  abort 'Error: no any environments found to load (./config/*.json)' if environments.empty?

  @config = []
  @requestParameters = []

  if ENV['RAKE_ENV'] # create/migrate rake task
    if environments.include? ENV['RAKE_ENV']
      @config = JSON.parse(File.read("./config/#{ENV['RAKE_ENV']}.json"))
      @config[:env] = ENV['RAKE_ENV']
    else 
      puts "Error: no such environment found: #{ENV['RAKE_ENV']}"
      puts 'Available options to use:'
      environments.each do |env|
        puts "rake [task] RAKE_ENV=#{env}"
      end
      exit
    end
  else
    if environments.include? ENV['RACK_ENV']
      @requestParameters = JSON.parse(File.read("./config/requestParameters.cfg"))
      @config = JSON.parse(File.read("./config/#{ENV['RACK_ENV']}.json"))
      @config[:env] = ENV['RACK_ENV']
      @config['campAPI']['defaultSpdrProvisioningMins'] = ENV['minsSPDR'].to_i if ENV['minsSPDR']
      @config['campAPI']['defaultSpdrErrorProvisioningMins'] = ENV['minsSPDRError'].to_i if ENV['minsSPDRError']
    else
      puts "Error: no such environment found: #{ENV['RACK_ENV']}"
      puts 'Available options to use:'
      environments.each do |env|
        puts "rackup -E #{env}"
      end
      exit
    end

    unless ENV['CAMP_KEY']
      abort 'Error: CAMP_KEY environment variable is not set'
    end

  end 

  def self.containsBaseEntitlement(raEntitlements)
    if @config && @config['entitlementAddons']
      raEntitlements.each do |entitlement|
        return true unless (@config['entitlementAddons'].include? entitlement["product"])
      end
    end
    false
  end

  def self.isBrandWithSPDR(brand)
    @config && @config['brandsWithSPDR'] && (@config['brandsWithSPDR'].include? brand)
  end

  def self.config
    @config.clone
  end

  def self.requestParameters
    @requestParameters.clone
  end

  def self.isHyphensInjectionRequired?(guid)
#    return guid == /[0-9a-fA-F]{32}|[0-9a-fA-F\-]{36}/.match(guid).to_s
    return /[0-9a-fA-F]{32}/.match(guid).to_s == guid
  end

  def self.version
    '1.0.32'
  end

end