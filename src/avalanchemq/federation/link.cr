require "../observable"
require "./publisher"
require "./consumer"
require "./queue_upstream"
require "./exchange_upstream"
require "../sortable_json"

module AvalancheMQ
  module Federation
    class Upstream
      class Link
        include Observer
        include SortableJSON
        Log = ::Log.for(self)
        getter connected_at

        @publisher : Publisher?
        @consumer : Consumer?
        @state = 0_u8
        @consumer_available = Channel(Nil).new
        @done = Channel(Nil).new
        @connected_at : Int64?

        def initialize(@upstream : QueueUpstream, @federated_q : Queue)
          @federated_q.register_observer(self)
          @consumer_available.send(nil) if @federated_q.immediate_delivery?
        end

        def state
          @state.to_s
        end

        def name
          @federated_q.name
        end

        def on(event, data)
          Log.debug { "event=#{event} data=#{data}" }
          case event
          when :delete, :close
            @upstream.stop_link(@federated_q)
          when :add_consumer
            @consumer_available.send(nil)
          when :rm_consumer
            nil
          else raise "Unexpected event '#{event}'"
          end
        end

        def details_tuple
          {
            upstream:  @upstream.name,
            vhost:     @upstream.vhost.name,
            timestamp: @connected_at ? Time.unix_ms(@connected_at.not_nil!) : nil,
            type:      @upstream.is_a?(QueueUpstream) ? "queue" : "exchange",
            uri:       @upstream.uri.to_s,
            resource:  @federated_q.name,
          }
        end

        def run
          Log.info { "Starting" }
          spawn(run_loop, name: "Federation link #{@upstream.vhost.name}/#{@federated_q.name}")
          Fiber.yield
        end

        private def run_loop
          loop do
            break if stopped?
            @state = State::Starting
            if !@federated_q.immediate_delivery?
              Log.debug { "Waiting for consumers" }
              @consumer_available.receive?
              break if stopped?
            end
            @publisher = Publisher.new(@upstream, @federated_q)
            @consumer = Consumer.new(@upstream)
            p = @publisher.not_nil!
            c = @consumer.not_nil!
            c.on_frame { |f| p.forward f }
            p.on_frame { |f| c.forward f }
            p.run
            c.run
            @state = State::Running
            @connected_at = Time.utc.to_unix_ms
            @done.receive
            break
          rescue ex
            @connected_at = nil
            case ex
            when AMQP::Error::FrameDecode, Connection::UnexpectedFrame
              Log.warn { "Federation link failure: #{ex.cause.inspect}" }
            else
              Log.warn(exception: ex) { "Federation link: #{ex.inspect_with_backtrace}" }
            end
            @consumer.try &.close("Federation link stopped")
            @publisher.try &.close("SFederation link stopped")
            break if stopped?
            sleep @upstream.reconnect_delay.seconds
          end
          Log.info { "Federation link stopped" }
        ensure
          @connected_at = nil
        end

        # Does not trigger reconnect, but a graceful close
        def stop
          Log.info { "Stopping" }
          @state = State::Terminated
          @federated_q.unregister_observer(self)
          @consumer.try &.close("Federation link stopped")
          @publisher.try &.close("Federation link stopped")
          done!
        end

        def stopped?
          @state == State::Terminated
        end

        private def done!
          select
          when @done.send(nil)
          else
          end
        end

        enum State
          Starting
          Running
          Terminated
        end
      end
    end
  end
end
