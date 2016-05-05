require './models/request.rb'

class RequestHeartbeat < Request

  def initialize (params)
    super :heartbeat, params
  end

end