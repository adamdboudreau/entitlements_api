require 'json'
require 'singleton'

class Cfg
  include Singleton

  environments = Dir.glob("./data/config/*.json").select{ |f| File.file? f }.map { |f| File.basename(f, ".*" ) }
  if environments.empty?
  	puts "Error: no any environments found to load (./data/config/*.json)"
  	exit
  end
  unless environments.include? ENV['RACK_ENV']
  	puts "Error: no such environment found: #{ENV['RACK_ENV']}"
  	puts "Available options to use:"
  	environments.each do |env|
      puts "rackup -E #{env}"
    end
    exit
  end 
  @config = JSON.parse(File.read("./data/config/#{ENV['RACK_ENV']}.json"))
  @config['env'] = env

  def self.config
  	@config
  end

end