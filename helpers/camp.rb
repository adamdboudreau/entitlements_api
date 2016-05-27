class CAMP

  def check?(guid)
    $logger.debug "\nCAMP.check started with guid=#{guid}\n"

    uri = URI.parse(Cfg.config['campAPI']['url'] + guid)
    pem = File.read(Cfg.config['campAPI']['pemFile'])
    key = File.read(Cfg.config['campAPI']['keyFile'])
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = true
    http.cert = OpenSSL::X509::Certificate.new(pem)
    http.key = OpenSSL::PKey::RSA.new(key)
    http.verify_mode = OpenSSL::SSL::VERIFY_PEER
    response = Hash.from_xml(http.request(Net::HTTP::Get.new(uri.request_uri)).body)
    response['SPDRNHL'] && response['SPDRNHL']['Status'] && (response['SPDRNHL']['Status']['code'] == '200')
  end

end
