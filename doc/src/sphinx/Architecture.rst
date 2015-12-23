Architecture
============

These are the components that make up a fully fledged tracing system.

.. image:: _static/architecture-0.png

Instrumented libraries
----------------------

Tracing information is collected on each host using the instrumented libraries
and sent to Zipkin. When the host makes a request to another service, it passes
a few tracing identifers along with the request so we can later tie the data
together.

.. image:: _static/architecture-1.png

We have instrumented the libraries below to trace requests and to pass the
required identifiers to the other services called in the request.

Finagle
-------

Finagle_ is an asynchronous network stack for the JVM that you can use to build
asynchronous Remote Procedure Call (RPC) clients and servers in Java, Scala, or
any JVM-hosted language.

Finagle_ is used heavily inside of Twitter and it was a natural point to include
tracing support. So far we have client/server support for Thrift and HTTP as well
as client only support for Memcache and Redis.

To set up a Finagle server in Scala, just do the following. Adding tracing is as
simple as adding finagle-zipkin_ as a dependency and a `tracer` to the ServerBuilder.

.. parsed-literal::
    ServerBuilder()
      .codec(ThriftServerFramedCodec())
      .bindTo(serverAddr)
      .name("servicename")
      .tracer(ZipkinTracer.mk())
      .build(new SomeService.FinagledService(queryService, new TBinaryProtocol.Factory()))

The tracing setup for clients is similar. When you've specified the Zipkin tracer
as above a small sample of your requests will be traced automatically. We'll
record when the request started and ended, services and hosts involved.

In case you want to record additional information you can add a custom annotation
in your code.

.. parsed-literal::
    Trace.record("starting that extremely expensive computation")

The line above will add an annotation with the string attached to the point in time
when it happened. You can also add a key value annotation. It could look like this:

.. parsed-literal::
    Trace.recordBinary("http.response.code", "500")

Ruby Thrift
-----------

There's a gem_ we use to trace requests. In order to push the tracer and generate
a trace id on a request you can use that gem in a RackHandler

For tracing client calls from Ruby we rely on the Twitter `Ruby Thrift client`_.
See below for an example on how to wrap the client.

.. parsed-literal::
    client = ThriftClient.new(SomeService::Client, "127.0.0.1:1234")
    client_id = FinagleThrift::ClientId.new(:name => "service_example.sample_environment")
    FinagleThrift.enable_tracing!(client, client_id), "service_name")

Transport
---------

Spans must be transported from the services being traced to Zipkin collectors.
There are two primary transports, Scribe and Kafka. Scribe is deprecated.

Zipkin Collector
----------------

Once the trace data arrives at the Zipkin collector daemon we check that it's
valid, store it and the index it for lookups.

Storage
-------

We originally built Zipkin on Cassandra for storage. It's scalable, has a
flexible schema, and is heavily used within Twitter. However, we made this
component pluggable, and we now have support for Redis and MySQL.

Zipkin Query Service
--------------------

Once the data is stored and indexed we need a way to extract it. This is where
the query daemon comes in, providing a simple JSON api for finding and retrieving
traces. The primary consumer of this api is the Web UI.

Web UI
------

A GUI that presents a nice face for viewing traces. The web UI provides a
method for viewing traces based on service, time, and  annotations. Note
that there is no built in authentication in the UI.

.. _Finagle: http://twitter.github.io/finagle
.. _finagle-zipkin: https://github.com/twitter/finagle/tree/master/finagle-zipkin
.. _gem: https://rubygems.org/gems/finagle-thrift
.. _Ruby thrift client: https://github.com/twitter/thrift_client
