# TDEngine Client for Erlang

An Erlang client for TDEngine, supporting both TDEngine Server and TDEngine Cloud.

## Features

- Support for both TDEngine Server and TDEngine Cloud
- Connection pooling
- Automatic retry on failures
- Support for both Basic auth and Cloud token auth

## Usage

### Connecting to TDEngine Server

```erlang
% Start the client with username/password authentication
Opts = [{host, <<"127.0.0.1">>},
        {port, 6041},
        {username, <<"root">>},
        {password, <<"taosdata">>}],
{ok, Pid} = tdengine:start_link(Opts).

% Execute SQL
SQL = <<"CREATE DATABASE test">>,
{ok, _} = tdengine:insert(Pid, SQL, []).
```

### Connecting to TDEngine Cloud

```erlang
% Start the client with cloud token
Opts = [{host, <<"cloud.tdengine.com">>},
        {https_enabled, true},
        {token, <<"your-cloud-token">>}],
{ok, Pid} = tdengine:start_link(Opts).

% Execute SQL with database name
SQL = <<"INSERT INTO readings VALUES (NOW, 23.5)">>,
{ok, _} = tdengine:insert(Pid, SQL, [{db_name, <<"iot_data">>}]).
```

## Configuration Options

- `host`: String or binary, TDEngine server host or cloud endpoint
- `port`: Integer, server port (optional for cloud endpoints)
- `username`: String or binary, username for basic auth (server only)
- `password`: String or binary, password for basic auth (server only)
- `token`: String or binary, token for cloud authentication (cloud only)
- `https_enabled`: Boolean, whether to use HTTPS (default: false, recommended true for cloud)
- `pool`: Atom, connection pool name (default: default)

## Authentication

The client supports two authentication methods:

1. Basic Authentication (TDEngine Server)
   - Uses username and password
   - Sends credentials via HTTP Basic Auth header

2. Token Authentication (TDEngine Cloud)
   - Uses token provided by TDEngine Cloud
   - Sends token via URL query parameter (?token=...)

## License

See LICENSE file for details.
