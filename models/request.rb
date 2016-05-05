class Request
#  attr_reader :type, :parameters, :response, :start_time

  def initialize (type, params)
    @start_time = Time.now.to_f
    @type = type
    @parameters = params
    @response = { success: "true", requestType: type, input: {} }
  end

  def process
    @parameters.each do |k, v| # return passed values
      @response[:input][k] = v
    end
    @response['requestTimeMs'] = '%.03f' % ((Time.now.to_f - @start_time)*1000)
    $logger.debug "\nRequest: /#{@type}/?" + URI.encode(@parameters.map{|k,v| "#{k}=#{v}"}.join("&")) + "\nResponse: #{@response.to_s}"
    @response
  end

end