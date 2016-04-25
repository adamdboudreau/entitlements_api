
Entitlements API service

Installation:
- cassandra db
- ruby 2.1.x
- gem install bundler
- bundle install

Setup:
- rake create -E [env]
- rake migrate -E [env]

Run:
- rackup -E [env]
where [env] is one of: dev, qa, prod, or any custom environment.
Make sure your environment settings at /data/config/[env].json are correct.

The service is accessible on port 9292.