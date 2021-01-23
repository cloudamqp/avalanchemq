require "logger"
require "../../sortable_json"

module AvalancheMQ
  class Client
    class Channel
      class Consumer
        include SortableJSON

        getter no_ack, queue, unacked, tag, exclusive, channel

        @log : Logger
        @unacked = 0_u32

        def initialize(@channel : Client::Channel, @tag : String,
                       @queue : Queue, @no_ack : Bool, @exclusive : Bool)
          @log = @channel.log.dup
          @log.progname += " consumer=#{@tag}"
        end

        def name
          @tag
        end

        def prefetch_count
          @channel.prefetch_count
        end

        def accepts?
          ch = @channel
          return false unless ch.client_flow?
          return true if prefetch_count.zero?
          unacked = ch.global_prefetch? ? ch.consumers.sum(&.unacked) : @unacked
          unacked < prefetch_count
        end

        def deliver(msg, sp, redelivered = false, recover = false)
          unless @no_ack || recover
            @unacked += 1
          end

          persistent = msg.properties.delivery_mode == 2_u8
          # @log.debug { "Getting delivery tag" }
          delivery_tag = @channel.next_delivery_tag(@queue, sp, persistent, @no_ack, self)
          # @log.debug { "Sending BasicDeliver" }
          deliver = AMQP::Frame::Basic::Deliver.new(@channel.id, @tag,
            delivery_tag,
            redelivered,
            msg.exchange_name, msg.routing_key)
          ok = @channel.client.deliver(deliver, msg)
          if ok
            if redelivered
              @channel.increment_redeliver
            else
              @channel.increment_deliver
            end
          end
          ok
        end

        def ack(sp)
          @unacked -= 1
        end

        def reject(sp)
          @unacked -= 1
        end

        def recover(requeue)
          @channel.recover(self) do |sp|
            if requeue
              reject(sp)
              @queue.reject(sp, requeue: true)
            else
              # redeliver to the original recipient
              env = @queue.read(sp)
              deliver(env.message, sp, true, recover: true)
            end
          end
        end

        def cancel
          @channel.send AMQP::Frame::Basic::Cancel.new(@channel.id, @tag, true)
          @channel.consumers.delete self
        end

        def details_tuple
          channel_details = @channel.details_tuple
          {
            queue: {
              name:  @queue.name,
              vhost: @queue.vhost.name,
            },
            consumer_tag:    @tag,
            exclusive:       @exclusive,
            ack_required:    !@no_ack,
            prefetch_count:  prefetch_count,
            channel_details: {
              peer_host:       channel_details[:connection_details][:peer_host]?,
              peer_port:       channel_details[:connection_details][:peer_port]?,
              connection_name: channel_details[:connection_details][:name],
              user:            channel_details[:user],
              number:          channel_details[:number],
              name:            channel_details[:name],
            },
          }
        end

        def save_transient_state(json)
          json.object do
            json.field "tag", @tag
            json.field "queue", @queue.name
            json.field "no_ack", @no_ack
            json.field "exclusive", @exclusive
          end
        end
      end
    end
  end
end
