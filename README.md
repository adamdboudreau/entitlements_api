
Entitlements API service

Installation:
- cassandra db
- ruby 2.1.x
- gem install bundler
- bundle install

Setup:
- rake create RAKE_ENV=[env]
- rake migrate RAKE_ENV=[env]

Run:
- rackup -E [env]
where [env] is one of available environment files (/config/[env].json).

The service is accessible on port 9292.