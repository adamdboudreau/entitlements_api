class CAMP

  def check (guid)
    $logger.debug "\nCAMP.check started\n"

    $logger.debug "OpenSSL settings: " + OpenSSL.inspect

    uri = URI.parse(Cfg.config['campAPI']['url'] + guid)
    $logger.debug "\nCAMP.check going to ping URL=#{uri}\n"
    pem = File.read(Cfg.config['campAPI']['pemFile'])
    key = ENV['CAMP_KEY'] ? ENV['CAMP_KEY'] : File.read(Cfg.config['campAPI']['keyFile'])
    $logger.debug "\nCAMP.check pem size=#{pem.size}\n"
    $logger.debug "\nCAMP.check pkey size=#{key.size}\n"
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = true
    http.cert = OpenSSL::X509::Certificate.new(pem)
    http.key = OpenSSL::PKey::RSA.new(key)
    http.verify_mode = OpenSSL::SSL::VERIFY_PEER

    nAttempt = 0
    response, result = nil

    begin
      result = nil
      nAttempt += 1
      $logger.debug "\nCAMP.check going to connect\n"
      response = Hash.from_xml(http.request(Net::HTTP::Get.new(uri.request_uri)).body)
      $logger.debug "\nCAMP.check pinging SPDR, attempt #{nAttempt}, response=#{response}\n"

      Cfg.config['campAPI']['rules'].each do |condition, rule|
        unless result
          path, value = condition.split('=')
          result = rule if getValue(response,path) == value
        end
      end
      result = Cfg.config['campAPI']['ruleDefault'] unless result
      $logger.debug "\nCAMP.check pinging SPDR, attempt #{nAttempt}, result=#{result}\n"
    end until nAttempt>result['redo']

    entitlementName = getValue(response, Cfg.config['campAPI']['entitlementPath'])
    entitlementName.downcase! if entitlementName
    result['entitlements'] = (entitlementName && Cfg.config['campAPI']['entitlementsMap'].key?(entitlementName)) ? Cfg.config['campAPI']['entitlementsMap'][entitlementName] : Cfg.config['campAPI']['entitlementsMap']['default']
    $logger.debug "\nCAMP.check returns #{result}\n"
    result
  end

  def getValue (hash, path)
    path.split('/').each do |step|
      return nil unless hash && hash[step]
      hash = hash[step]
    end
    hash
  end

  def getEntitlementParamsToInsert (guid)
    results = []
    spdrResults = check guid
    
    spdrResults['entitlements'].each do |entitlement|
      results << Hash[
        'guid'=>guid, 
        'brand'=>'gcl', 
        'product'=>entitlement, 
        'source'=>spdrResults['source'], 
        'trace_id'=>guid,
        'start_date'=>Time.now.to_i.to_s,
        'end_date'=>(Time.now + 60*Cfg.config['campAPI'][spdrResults['provisionTime']]).to_i.to_s
      ]
    end if spdrResults['entitled']
    results
  end

end
