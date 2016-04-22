
Entitlements API service

Installation:
- cassandra db
- ruby 2.1.x
- gem install bundler
- bundle install

Setup:
- rake db:create db:migrate
- RACK_ENV=[env] rackup
where [env] is one of: dev, qa, prod, or any custom environment (make sure you have environment settings at /data/config/[env].json)