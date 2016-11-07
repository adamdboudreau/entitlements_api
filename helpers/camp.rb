class CAMP

  def check (params)
    puts "CAMP.check started with params=#{params}"
    uri = URI.parse(Cfg.config['campAPI']['url'] + params['guid'].strip)
    pem = File.read(Cfg.config['campAPI']['pemFile'])
    key = ENV['CAMP_KEY']
    puts "CAMP.check pkey size=#{key.to_s.size}"
    http = Net::HTTP.new(uri.host, uri.port)
    puts "CAMP.check http object created"
    http.use_ssl = true
    http.ciphers = 'DEFAULT:!DH'
    http.cert = OpenSSL::X509::Certificate.new(pem)
    puts "CAMP.check http.cert is ok"
    http.key = OpenSSL::PKey::RSA.new(key)
    puts "CAMP.check http.key is ok"
    http.verify_mode = OpenSSL::SSL::VERIFY_NONE

    nAttempt = 0
    response, result = nil

    begin
      result = nil
      nAttempt += 1
      puts "CAMP.check going to connect to #{uri}"
      response = Hash.from_xml(params['spdrResponse'] ? params['spdrResponse'] : http.request(Net::HTTP::Get.new(uri.request_uri)).body)
      puts "CAMP.check params[spdrResponse] found, taking it as a response: #{params['spdrResponse']}" if params['spdrResponse']
      puts "CAMP.check pinging SPDR, attempt #{nAttempt}, response=#{response}" unless params['spdrResponse']

      Cfg.config['campAPI']['rules'].clone.each do |rule|
        puts "CAMP.check checking rule: #{rule}"
        unless result
          path, value = rule[0].split('=')
          result = rule[1].clone if (getValue(response,path) == value)
          puts "CAMP.check during checking the rule, result=#{result}"
        end
      end
      result = Cfg.config['campAPI']['ruleDefault'].clone unless result
      puts "CAMP.check pinging SPDR, attempt #{nAttempt}, result=#{result}"
    end until nAttempt>result['redo']

    entitlementName = getValue(response, Cfg.config['campAPI']['entitlementPath'])
    entitlementName.downcase! if entitlementName
    
    if (!result['entitlements']) || (result['entitlements'].empty?)
      result['entitlements'] = (entitlementName && Cfg.config['campAPI']['entitlementsMap'][entitlementName]) ? Cfg.config['campAPI']['entitlementsMap'][entitlementName].clone : Cfg.config['campAPI']['entitlementsMap']['default'].clone
    end
    puts "CAMP.check finished with #{result}"
    result
  end

  def getEntitlements (params)
    puts "\nCAMP.getEntitlements started with params=#{params}\n"
    uri = URI.parse(Cfg.config['campAPI']['url'] + params['guid'].strip)
    puts "CAMP.getEntitlements uri=#{uri.inspect}\n"
    pem = File.read(Cfg.config['campAPI']['pemFile'])
    key = ENV['CAMP_KEY']
    puts "CAMP.getEntitlements pkey size=#{key.to_s.size}\n"
    http = Net::HTTP.new(uri.host, uri.port)
    puts "CAMP.getEntitlements http object created\n"
    http.use_ssl = true
    http.ciphers = 'DEFAULT:!DH'
    http.cert = OpenSSL::X509::Certificate.new(pem)
    puts "CAMP.getEntitlements http.cert is ok\n"
    http.key = OpenSSL::PKey::RSA.new(key)
    puts "CAMP.getEntitlements http.key is ok\n"
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
    spdrResults = self.check(params)
#    spdrResults = self.getEntitlements(params) # the change for RGCL3-607, removed now

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
