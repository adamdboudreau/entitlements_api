
Entitlements API service

Installation:
- cassandra db 3.x
- ruby 2.1.x
- gem install bundler
- bundle install

Setup:
- rake create migrate RAKE_ENV=[env]

Run:
- rackup -E [env]
where [env] is one of available environment files (/config/[env].json).

The service is accessible on port 9292.

Request [host]:9292/v1/heartbeat to check if it's up and running.