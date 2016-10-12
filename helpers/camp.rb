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

  def getEntitlements (params)
    puts "\nCAMP.getEntitlements started with params=#{params}\n"
    uri = URI.parse(Cfg.config['campAPI']['url'] + params['guid'].strip)
    pem = File.read(Cfg.config['campAPI']['pemFile'])
    key = ENV['CAMP_KEY']
    puts "\nCAMP.getEntitlements pkey size=#{key.to_s.size}\n"
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = true
    http.ciphers = 'DEFAULT:!DH'
    http.cert = OpenSSL::X509::Certificate.new(pem)
    http.key = OpenSSL::PKey::RSA.new(key)
    http.verify_mode = OpenSSL::SSL::VERIFY_NONE

    nAttempt = 0
    response, result = nil

    begin # check all the rules and set result as a rule value, or ruleDefault if not found
      result = nil
      nAttempt += 1
      puts "\nCAMP.getEntitlements going to connect to #{uri}\n"
      response = Hash.from_xml(params['spdrResponse'] ? params['spdrResponse'] : http.request(Net::HTTP::Get.new(uri.request_uri)).body)
      puts "\nCAMP.getEntitlements params[spdrResponse] found, taking it as a response: #{params['spdrResponse']}\n" if params['spdrResponse']
      puts "\nCAMP.getEntitlements pinging SPDR, attempt #{nAttempt}, response=#{response}\n" unless params['spdrResponse']

      Cfg.config['campAPI']['rules'].clone.each do |rule|
        unless result
          puts "CAMP.getEntitlements checking rule: #{rule}\n"
          path, value = rule[0].split('=')
          result = rule[1].clone if (getValue(response,path) == value)
          puts "CAMP.getEntitlements during checking the rule, result=#{result}\n"
        end
      end
      unless result
        puts "\nCAMP.getEntitlements cannot find rule, use default for SPDR response=#{response}\n"
        result = Cfg.config['campAPI']['ruleDefault'].clone
      end
      puts "\nCAMP.getEntitlements pinging SPDR, attempt #{nAttempt}, result=#{result}\n"
    end until nAttempt>result['redo']
    result['entitlements'] = Array.new

    puts "CAMP.getEntitlements pre-result: #{result}"
    accounts = getValue(response, 'SPDR/Response/Account')
    accounts = (accounts==nil) ? Array.new : (accounts.kind_of?(Array) ? accounts : Array[accounts] )
    puts "CAMP.getEntitlements Accounts: " + accounts.inspect
    accounts.each do |account|
      puts "CAMP.getEntitlements Processing account: #{account}\nservices: #{account['Services']}"
      if result['entitlements'].count<3 &&
        account.key?("AccountStatus") && (account['AccountStatus'].downcase=='active') && 
        account.key?("AccountType") && (account['AccountType'].downcase!='zuora')

          result['entitlements'] = ['gameplus']
         
          if account.key?("Services")
            entitlements = account['Services']['Entitlement']
            entitlements = (entitlements==nil) ? Array.new : (entitlements.kind_of?(Array) ? entitlements : Array[entitlements] )
            entitlements.each do |entitlement|
              puts "CAMP.getEntitlements Processing entitlement: #{entitlement}"
              if entitlement.key?("Type") && (entitlement['Type'].downcase=='nhl') && 
                 entitlement.key?("SubType") && (entitlement['SubType'].downcase.include? 'free')
                  result['entitlements'] = ['gameplus','fullgcl','wch']
              end
            end
          end
      end
    end
    puts "CAMP.getEntitlements returns #{result}"
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
#    spdrResults = self.check(params) # WAS HERE BEFORE RGCL3-607, replaced by the next line
    spdrResults = self.getEntitlements(params)
    
    spdrResults['entitlements'].each do |entitlement|
      results << Hash[
        'guid'=>params['guid'], 
        'brand'=>'gcl', 
        'product'=>entitlement, 
        'source'=>spdrResults['source'], 
        'trace_id'=>params['guid'],
        'start_date'=>Time.now.to_i.to_s,
        'end_date'=>(entitlement=='gameplus') ? '1504137600' : (Time.now + 60*Cfg.config['campAPI'][spdrResults['provisionTime']]).to_i.to_s
      ]
    end if spdrResults['entitled']
    results
  end

end
