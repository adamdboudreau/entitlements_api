require 'json'
require 'singleton'

class Cfg
  include Singleton

  environments = Dir.glob('./config/*.json').select{ |f| File.file? f }.map { |f| File.basename(f, '.*' ) }
  if environments.empty?
    puts 'Error: no any environments found to load (./config/*.json)'
    exit
  end

  @config

  if ENV["RAKE_ENV"] # create/migrate rake task
    unless environments.include? ENV['RAKE_ENV']
      puts "Error: no such environment found: #{ENV['RAKE_ENV']}"
      puts 'Available options to use:'
      environments.each do |env|
        puts "rake [task] RAKE_ENV=#{env}"
      end
      exit
    else 
      @config = JSON.parse(File.read("./config/#{ENV['RAKE_ENV']}.json"))
      @config['env'] = ENV['RAKE_ENV']
    end
  else
    unless environments.include? ENV['RACK_ENV']
      puts "Error: no such environment found: #{ENV['RACK_ENV']}"
      puts 'Available options to use:'
      environments.each do |env|
        puts "rackup -E #{env}"
      end
      exit
    else 
      @config = JSON.parse(File.read("./config/#{ENV['RACK_ENV']}.json"))
      @config['env'] = ENV['RACK_ENV']
    end
  end 

  def self.config
    @config
  end

end