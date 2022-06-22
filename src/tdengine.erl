-module(tdengine).

-behaviour(gen_server).

%% API.
-export([ start_link/0
        , start_link/1
        , stop/1
        ]).

-export ([ insert/3
         ]).

%% gen_server.
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {url, username, password, pool}).

%% API.
-spec start_link() -> {ok, pid()}.
start_link() ->
    start_link([]).

start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

stop(Pid) ->
    gen_server:stop(Pid).

insert(Pid, SQL, Opts) ->
    gen_server:call(Pid, {insert, SQL, Opts}).

%% gen_server.
init([Opts]) ->
    %% hackney_pool impl. has bottlenck on pool
    %% worker selection (checkin/checkout) via single gen_server process. with
    %% many tdengine workers calling the pool process will easily build up a queue
    %% causing long blocking on the worker side that delays the handling of the
    %% caller.
    %% Here we have a connection pool per tdengine worker
    PoolName = {proplists:get_value(pool, Opts, default), self()},
    hackney_pool:start_pool(PoolName, [{max_connections, 4}]),
    State = #state{url = make_url(Opts),
                   username = proplists:get_value(username, Opts, ""),
                   password =  proplists:get_value(password, Opts, ""),
                   pool = PoolName
                  },
    {ok, State}.

handle_call({insert, SQL, _Opts}, _From, State = #state{url = Url,
                                                        username = Username,
                                                        password =  Password,
                                                        pool = Pool}) ->
    Reply = query(Pool, Url, Username, Password, SQL),
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

query(Pool, Url, Username, Password, SQL) ->
    Token = base64:encode(<<Username/binary, ":", Password/binary>>),
    Headers = [{<<"Authorization">>, <<"Basic ", Token/binary>>}],
    Options = [{pool, Pool},
               {connect_timeout, 10000},
               {recv_timeout, 30000},
               {follow_redirectm, true},
               {max_redirect, 5},
               with_body],
    case hackney:request(post, Url, Headers, SQL, Options) of
        {ok, StatusCode, _Headers, ResponseBody}
          when StatusCode =:= 200 orelse StatusCode =:= 204 ->
            {ok, StatusCode, ResponseBody};
        {ok, StatusCode, _Headers, ResponseBody} ->
            {error, {StatusCode, ResponseBody}};
        {error, Reason} ->
            {error, Reason}
    end.

make_url(Opts) ->
    Host = case proplists:get_value(host, Opts, <<"127.0.0.1">>) of
        <<"http://", Host0/binary>> -> binary_to_list(Host0);
        <<"https://", Host0/binary>> -> binary_to_list(Host0);
        Host0 -> binary_to_list(Host0)
    end,
    Port = integer_to_list(proplists:get_value(port, Opts, 6041)),
    Scheme = case proplists:get_value(https_enabled, Opts, false) of
                true -> "https://";
                false -> "http://"
            end,
    Scheme ++ Host ++ ":" ++ Port ++ "/rest/sql".
