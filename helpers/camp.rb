class CAMP

  def check (params)
    $logger.debug "\nCAMP.check started with params=#{params}\n"
    uri = URI.parse(Cfg.config['campAPI']['url'] + params['guid'].strip)
    pem = File.read(Cfg.config['campAPI']['pemFile'])
    key = ENV['CAMP_KEY']
    $logger.debug "\nCAMP.check pkey size=#{key.to_s.size}\n"
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = true
    http.ciphers = 'DEFAULT:!DH'
    http.cert = OpenSSL::X509::Certificate.new(pem)
    http.key = OpenSSL::PKey::RSA.new(key)
    http.verify_mode = OpenSSL::SSL::VERIFY_NONE

    nAttempt = 0
    response, result = nil

    begin
      result = nil
      nAttempt += 1
      $logger.debug "\nCAMP.check going to connect to #{uri}\n"
      response = Hash.from_xml(params['spdrResponse'] ? params['spdrResponse'] : http.request(Net::HTTP::Get.new(uri.request_uri)).body)
      $logger.debug "\nCAMP.check params[spdrResponse] found, taking it as a response: #{params['spdrResponse']}\n" if params['spdrResponse']
      $logger.debug "\nCAMP.check pinging SPDR, attempt #{nAttempt}, response=#{response}\n" unless params['spdrResponse']

      Cfg.config['campAPI']['rules'].clone.each do |rule|
        $logger.debug "\nCAMP.check checking rule: #{rule}\n"
        unless result
          path, value = rule[0].split('=')
          result = rule[1].clone if (getValue(response,path) == value)
          $logger.debug "\nCAMP.check during checking the rule, result=#{result}\n"
        end
      end
      result = Cfg.config['campAPI']['ruleDefault'].clone unless result
      $logger.debug "\nCAMP.check pinging SPDR, attempt #{nAttempt}, result=#{result}\n"
    end until nAttempt>result['redo']

    entitlementName = getValue(response, Cfg.config['campAPI']['entitlementPath'])
    entitlementName.downcase! if entitlementName
    
    if (!result['entitlements']) || (result['entitlements'].empty?)
      result['entitlements'] = (entitlementName && Cfg.config['campAPI']['entitlementsMap'][entitlementName]) ? Cfg.config['campAPI']['entitlementsMap'][entitlementName].clone : Cfg.config['campAPI']['entitlementsMap']['default'].clone
    end
    $logger.debug "\nCAMP.check finished with #{result}"
    result
  end

  def getValue (hash, path)
    path.split('/').each do |step|
      return nil unless hash && hash[step]
      hash = hash[step]
    end
    hash
  end

  def getEntitlementParamsToInsert (params)
    results = []
    spdrResults = self.check(params)
    
    spdrResults['entitlements'].each do |entitlement|
      results << Hash[
        'guid'=>params['guid'], 
        'brand'=>'gcl', 
        'product'=>entitlement, 
        'source'=>spdrResults['source'], 
        'trace_id'=>params['guid'],
        'start_date'=>Time.now.to_i.to_s,
        'end_date'=>(Time.now + 60*Cfg.config['campAPI'][spdrResults['provisionTime']]).to_i.to_s
      ]
    end if spdrResults['entitled']
    results
  end

end
