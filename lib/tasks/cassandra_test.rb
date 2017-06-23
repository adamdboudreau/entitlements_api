desc 'cassandra_test'
task :cassandra_test do
  start_date_min = (Time.now-24.hours).beginning_of_day.strftime("%F %T%z")
  start_date_max = (Time.now-24.hours).end_of_day.strftime("%F %T%z")
  raResults = Array.new
  cql = "SELECT COUNT(*) FROM #{Cfg.config['tables']['entitlements']} WHERE start_date>='#{start_date_min}' AND start_date<='#{start_date_max}' ALLOW FILTERING"
  puts "cassandra_test :: Cassandra integrity test task started, CQL to test: #{cql}"
  Cfg.config['cassandraCluster']['hosts'].each do |host|
    puts "cassandra_test :: Trying host #{host}"
    cluster = { hosts: [host], port: Cfg.config['cassandraCluster']['port'], timeout: 30 }
    cluster[:username] = Cfg.config['cassandraCluster']['username'] unless Cfg.config['cassandraCluster']['username'].empty?
    cluster[:password] = Cfg.config['cassandraCluster']['password'] unless Cfg.config['cassandraCluster']['password'].empty?
    if Cfg.config['cassandraCluster']['use_ssl']
      cluster[:server_cert] = Cfg.config['cassandraCluster']['certServer']
      cluster[:client_cert] = Cfg.config['cassandraCluster']['certClient']
      cluster[:private_key] = Cfg.config['cassandraCluster']['certKey']
      cluster[:passphrase] = ENV['CASSANDRA_PASSPHRASE']
    end

    begin
      puts "cassandra_test :: Going to connect to cluster #{cluster}"
      session = Cassandra.cluster(cluster).connect Cfg.config['cassandraCluster']['keyspace']
      puts "cassandra_test :: Connected to #{host}"
#      result = session.execute("USE #{Cfg.config['cassandraCluster']['keyspace']}")
#      puts "cassandra_test :: keyspace changed ok to #{Cfg.config['cassandraCluster']['keyspace']}"
      result = session.execute(cql, timeout: 30)
      puts "cassandra_test :: Got result: #{result.length}"
    rescue Exception => e
      puts "cassandra_test :: ERROR! EXCEPTION: #{e.message}\nBacktrace: #{e.backtrace.inspect}"
    end
  end
end
