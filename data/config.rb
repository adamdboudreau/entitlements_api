require 'json'
require 'singleton'

class Cfg
  include Singleton
  env = ENV['RACK_ENV'] || :dev
  @config = JSON.parse(File.read('./data/config/' + env + '.json'))
  @config['env'] = env

  def self.config
  	@config
  end

end