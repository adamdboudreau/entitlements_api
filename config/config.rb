require 'cassandra'
require 'grape'
require 'json'
require 'singleton'
require 'net/http'
require 'uri'

require './lib/connection.rb'
require './lib/entitlements_service.rb'
require './helpers/request.rb'
require './helpers/camp.rb'

class Cfg
  include Singleton

  environments = Dir.glob('./config/*.json').select{ |f| File.file? f }.map { |f| File.basename(f, '.*' ) }
  abort 'Error: no any environments found to load (./config/*.json)' if environments.empty?

  @config = []

  if ENV['RAKE_ENV'] # create/migrate rake task
    if environments.include? ENV['RAKE_ENV']
      @config = JSON.parse(File.read("./config/#{ENV['RAKE_ENV']}.json"))
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
      @config = JSON.parse(File.read("./config/#{ENV['RACK_ENV']}.json"))
    else
      puts "Error: no such environment found: #{ENV['RACK_ENV']}"
      puts 'Available options to use:'
      environments.each do |env|
        puts "rackup -E #{env}"
      end
      exit
    end
  end 

  def self.config
    @config
  end

end