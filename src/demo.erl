-module(demo).

-export([
    start/0,
    client/0,
    opts/0,
    callback/1
]).

client() ->
    Name = "c-" ++ integer_to_list(erlang:unique_integer([positive])),
    {ok, Client} = hstreamdb_client:start(Name, opts()),
    logger:debug("[hstreamdb] start client  ~p~n", [Client]),
    Client.

opts() ->
    #{
        % url => "http://127.0.0.1:6570",
        % url => "http://192.168.1.2:26570",
        url => "https://192.168.1.2:6570",
        rpc_options => #{
            pool_size => 50,
            gun_opts => #{
                transport => tls,
                transport_opts => [{cacertfile, "/root/certs/root_ca.crt"},
                                   {verify, verify_none}
                                  ]
            }
        }
        %% host_mapping => #{
        %%     <<"10.5.0.4">> => <<"127.0.0.1">>,
        %%     <<"10.5.0.5">> => <<"127.0.0.1">>
        %% }
    }.

start() ->
    Stream = "demo_" ++ integer_to_list(erlang:system_time(millisecond)),

    _ = application:ensure_all_started(hstreamdb_erl),

    Client = client(),
    Echo = hstreamdb_client:echo(Client),
    logger:debug("[hstreamdb] echo: ~p~n", [Echo]),

    % CreateStream = hstreamdb_client:create_stream(Client, Stream, 1, 24 * 60 * 60, 1),
    CreateStream = hstreamdb_client:create_stream(Client, Stream, 2, 24 * 60 * 60, 1),
    logger:debug("[hstreamdb] create_stream: ~p~n", [CreateStream]),
    timer:sleep(1000),

    ProducerOptions = [
        {pool_size, 4},
        {stream, Stream},
        % {callback, {producer_example, callback}},
        {max_records, 1},
        {interval, 1}
    ],
    Producer = test_producer,
    {ok, test_producer} = hstreamdb:start_producer(Client, Producer, ProducerOptions),
    logger:debug("[hstreamdb] start producer  ~p~n", [Producer]),

    PartitioningKey = "ok1",
    PayloadType = raw,
    % Payload = <<"hello stream !">>,
    Payload = crypto:strong_rand_bytes(10240),
    Record = hstreamdb:to_record(PartitioningKey, PayloadType, Payload),
    logger:debug("[hstreamdb] to record ~p~n", [Record]),

    %% _ = hstreamdb:append_sync(Producer, Record),
    do_n(
        10000,
        fun() ->
            Begin_ts = erlang:system_time(millisecond),
            _ = hstreamdb:append_sync(Producer, Record),
            End_ts = erlang:system_time(millisecond),
            Tt = End_ts - Begin_ts,
            io:format("append latency: ~p ms~n", [Tt]),
            timer:sleep(200)
        end
    ),

    timer:sleep(1000),
    Stop = hstreamdb:stop_producer(Producer),
    logger:debug("[hstreamdb] stop producer  ~p~n", [Stop]),

    Client.

callback(_A) ->
    % logger:debug("[hstreamdb] callback ~p~n", [A]).
    ok.

do_n(N, _Fun) when N =< 0 -> ok;
do_n(N, Fun) when is_function(Fun, 0) ->
    _ = Fun(),
    do_n(N - 1, Fun);
do_n(N, Fun) when is_function(Fun, 1) ->
    _ = Fun(N),
    do_n(N - 1, Fun).

