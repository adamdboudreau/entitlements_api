class Request
#  attr_reader :type, :parameters, :response, :start_time

  def initialize (type, params = {}, httptype = :get)
    @start_time = Time.now.to_f
    @type = type
    @parameters = params
    @httptype = httptype
    @response = { success: "true" }
  end

  def process
    @response[:request] = {}
    @response[:request][:type] = "#{@httptype.upcase} #{@type}"
    @response[:request][:input] = {}
    @parameters.each do |k, v| # return passed values
      @response[:request][:input][k] = v
    end
    @response[:request][:processingTimeMs] = '%.03f' % ((Time.now.to_f - @start_time)*1000)
    $logger.debug "\nRequest: /#{@type}/?" + URI.encode(@parameters.map{|k,v| "#{k}=#{v}"}.join("&")) + "\nResponse: #{@response.to_s}"
    @response
  end

end