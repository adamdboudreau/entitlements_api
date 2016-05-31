
## Entitlements API service


### Description
This REST service provides an interface to Cassandra database to get/set entitlements for Rogers GameCenterLive products.


### API requests
- GET /v1/entitlements - Gets a list of entitlements for a user/brand/product
- DELETE /v1/entitlements - Cancels the entitlement(s) for a certain guid/brand/product/type
- PUT /v1/entitlement - Creates or updates entitlement
- GET /v1/tc - Gets the accepted terms and conditions information
- PUT /v1/tc - Creates or updates terms and conditions acceptance information
- GET /v1/archive - Returns archived (expired, deleted, updated) entitlements from history table
- POST /v1/archive - Performs entitlements table cleanup moving expired entitlements to history table. Supposed to be used by nightly cron job.
- GET /v1/heartbeat - Pings the service to make sure it's up and running


### Installation:
- cassandra db 3.x
- ruby 2.1.x
- gem install bundler
- bundle install


### Config files
One or more environments can be setup through config files which are /config/[env].json.
Every time you run the application specifying an existing environment.


### Setup:
- rake delete create migrate RAKE_ENV=[env]


### Run:
- rackup -E [env]

where [env] is one of available environment files (/config/[env].json).

By default the service is accessible on port 9292.

Request [host]:[port]/v1/heartbeat to check if it's up and running.

### More info

More detailed documentation can be found at [Rogers Confluence page](https://rogers.atlassian.net/wiki/display/ARC/DTC+Entitlement+Service+API+specification).
