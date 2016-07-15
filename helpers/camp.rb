class CAMP

  def check (guid)
    uri = URI.parse(Cfg.config['campAPI']['url'] + guid.strip)
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
    rules = Cfg.config['campAPI']['rules'].clone

    begin
      result = nil
      nAttempt += 1
      $logger.debug "\nCAMP.check going to connect to #{uri}\n"
      response = Hash.from_xml(http.request(Net::HTTP::Get.new(uri.request_uri)).body)
#      response['CAMPNHL']['Status']['code'] = "402"
      $logger.debug "\nCAMP.check pinging SPDR, attempt #{nAttempt}, response=#{response}\n"
      $logger.debug "\nCAMP.check pinging SPDR, attempt #{nAttempt}, rules=#{rules}\n"

      rules.each do |rule|
        $logger.debug "\nCAMP.check checking rule: #{rule}\n"
        unless result
          path, value = rule[0].split('=')
          result = rule[1] if (getValue(response,path) == value)
          $logger.debug "\nCAMP.check during checking the rule, result=#{result}\n"
        end
      end
      $logger.debug "\nCAMP.check checked all rules, result=#{result}\n"
      result = Cfg.config['campAPI']['ruleDefault'] unless result
      $logger.debug "\nCAMP.check pinging SPDR, attempt #{nAttempt}, result=#{result}\n"
    end until nAttempt>result['redo']

    $logger.debug "\nCAMP.check finished pinging SPDR, attempt #{nAttempt}, result=#{result}\n"
    entitlementName = getValue(response, Cfg.config['campAPI']['entitlementPath'])
    entitlementName.downcase! if entitlementName
    $logger.debug "\nCAMP.check entitlementName=#{entitlementName}\n"
    $logger.debug "\nCAMP.check has key=#{Cfg.config['campAPI']['entitlementsMap'].key?(entitlementName)}\n"
    $logger.debug "\nCAMP.check Cfg.config['campAPI']['entitlementsMap'][#{entitlementName}]=#{Cfg.config['campAPI']['entitlementsMap'][entitlementName]}\n"
    
    if (!result['entitlements']) || (result['entitlements'].empty?)
      result['entitlements'] = (entitlementName && Cfg.config['campAPI']['entitlementsMap'].key?(entitlementName)) ? Cfg.config['campAPI']['entitlementsMap'][entitlementName] : Cfg.config['campAPI']['entitlementsMap']['default']
    end
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
