-module(tdengine).

-behaviour(gen_server).

%% API.
-export([ start_link/0
        , start_link/1
        , stop/1
        ]).

-export ([ insert/3
         , insert/4
         ]).

%% gen_server.
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        , format_status/1
        ]).

-record(state, {url, username, password, token, pool}).

%% API.
-spec start_link() -> {ok, pid()}.
start_link() ->
    start_link([]).

start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

stop(Pid) ->
    gen_server:stop(Pid).

insert(Pid, SQL, QueryOpts) ->
    insert(Pid, SQL, QueryOpts, infinity).

insert(Pid, SQL, QueryOpts, CallTimeout) ->
    gen_server:call(Pid, {insert, SQL, QueryOpts}, CallTimeout).

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
                   password = proplists:get_value(password, Opts, ""),
                   token = proplists:get_value(token, Opts, ""),
                   pool = PoolName
                  },
    {ok, State}.

handle_call({insert, SQL, QueryOpts}, _From, State = #state{url = Url,
                                                        username = Username,
                                                        password = Password,
                                                        token = Token,
                                                        pool = Pool}) ->
    Reply = query(Pool, Url, Username, Password, Token, SQL, QueryOpts, undefined, 3),
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

format_status(#{state := State} = Status) ->
    Status#{state := State#state{password = <<"******">>}};
format_status(Status) ->
    Status.

query(_Pool, _Url, _Username, _Password, _Token, _SQL, _QueryOpts, Error, 0) -> Error;
query(Pool, Url, Username, Password, Token, SQL, QueryOpts, _LastError, Retry) ->
    case query(Pool, Url, Username, Password, Token, SQL, QueryOpts) of
        {error, _Reason} = Error ->
            query(Pool, Url, Username, Password, Token, SQL, QueryOpts, Error, Retry - 1);
        Reply ->
            Reply
    end.

query(Pool, Url, Username, Password, Token, SQL, QueryOpts) ->
    BaseUrl = maybe_append_dbname(Url, proplists:get_value(db_name, QueryOpts, <<"">>)),
    HasToken = not is_empty_str(Token),
    {Url1, Headers} = case HasToken of
        true ->
            {BaseUrl ++ "?token=" ++ str(Token), []};
        false ->
            BasicToken = base64:encode(<<Username/binary, ":", Password/binary>>),
            {BaseUrl, [{<<"Authorization">>, <<"Basic ", BasicToken/binary>>}]}
    end,
    Options = [{pool, Pool},
               {connect_timeout, 10000},
               {recv_timeout, 30000},
               {follow_redirect, true},
               {max_redirect, 5},
               with_body],
    case hackney:request(post, Url1, Headers, SQL, Options) of
        {ok, StatusCode, _Headers, ResponseBody}
          when StatusCode =:= 200 orelse StatusCode =:= 204 ->
            try
                jsx:decode(ResponseBody, [return_maps])
            of
                %% Compatible with TDengine 2.x returns
                #{<<"status">> := <<"succ">>} = ResponseMap ->
                    {ok, ResponseMap};
                %% Compatible with TDengine 3.x returns
                #{<<"code">> := 0} = ResponseMap ->
                    {ok, ResponseMap};
                ResponseMap ->
                    {error, ResponseMap}
            catch
                error:badarg ->
                    {error, {invalid_json, ResponseBody}}
            end;
        {ok, StatusCode, _Headers, <<>>} ->
            {error, format_error_msg(StatusCode)};
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
    Scheme = case proplists:get_value(https_enabled, Opts, false) of
                true -> "https://";
                false -> "http://"
            end,
    Port = case proplists:get_value(port, Opts, undefined) of
        undefined -> "";
        PortNum when is_integer(PortNum) -> ":" ++ integer_to_list(PortNum)
    end,
    Scheme ++ Host ++ Port ++ "/rest/sql".

maybe_append_dbname(URL, <<"">>) ->
    str(URL);
maybe_append_dbname(URL, DBName) ->
    str(URL) ++ "/" ++ str(DBName).

str(S) when is_binary(S) ->
    binary_to_list(S);
str(S) when is_list(S) ->
    S.

is_empty_str(undefined) ->
    true;
is_empty_str(S) when is_binary(S) ->
    S =:= <<>>;
is_empty_str(S) when is_list(S) ->
    S =:= [].

format_error_msg(StatusCode) ->
    %% ref: https://docs.tdengine.com/cloud/programming/client-libraries/rest-api/
    Msg = case StatusCode of
        400 -> <<"Parameter error">>;
        401 -> <<"Authentication failure">>;
        404 -> <<"Interface not found">>;
        500 -> <<"Internal error">>;
        503 -> <<"Insufficient system resources">>;
        _ -> <<"Unknown error">>
    end,
    {StatusCode, Msg}.
