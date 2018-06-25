require "../controller"

module AvalancheMQ
  class MainController < Controller
    private def register_routes
      get "/api/overview" do |context, _params|
        x_vhost = context.request.headers["x-vhost"]?
        channels = 0
        connections = 0
        exchanges = 0
        queues = 0
        consumers = 0
        vhosts(user(context)).each do |vhost|
          next unless x_vhost.nil? || vhost.name == x_vhost
          vhost_connections = @amqp_server.connections.select { |c| c.vhost.name == vhost.name }
          connections += vhost_connections.size
          channels += vhost_connections.reduce(0) { |memo, i| memo + i.channels.size }
          consumers += nr_of_consumers(vhost_connections)
          exchanges += vhost.exchanges.size
          queues += vhost.queues.size
        end

        {
          "avalanchemq_version": AvalancheMQ::VERSION,
          "object_totals":       {
            "channels":    channels,
            "connections": connections,
            "consumers":   consumers,
            "exchanges":   exchanges,
            "queues":      queues,
          },
          "listeners":      @amqp_server.listeners,
          "exchange_types": Exchange.types.map { |name| {"name": name} },
        }.to_json(context.response)
        context
      end

      get "/api/whoami" do |context, params|
        user(context).user_details.to_json(context.response)
        context
      end

      get "/api/aliveness-test/:vhost" do |context, params|
        with_vhost(context, params) do |vhost|
          @amqp_server.vhosts[vhost].declare_queue("aliveness-test", false, false)
          @amqp_server.vhosts[vhost].bind_queue("aliveness-test", "amq.direct", "aliveness-test")
          msg = Message.new(Time.utc_now.epoch_ms,
            "amq.direct",
            "aliveness-test",
            AMQP::Properties.new,
            4_u64,
            "test".to_slice)
          ok = @amqp_server.vhosts[vhost].publish(msg)
          env = @amqp_server.vhosts[vhost].queues["aliveness-test"].get(true)
          ok = env && String.new(env.message.body) == "test"
          {status: ok ? "ok" : "failed"}.to_json(context.response)
        end
      end

      get "/api/shovels" do |context, params|
        shovels = [] of Hash(String, String)
        vhosts(user(context)).each do |vhost|
          vhost.shovels.not_nil!.each do |shovel|
            shovels << {
              "name"  => shovel.name,
              "vhost" => vhost.name,
              "state" => shovel.state,
            }
          end
        end
        shovels.to_json(context.response)
        context
      end
    end

    private def nr_of_consumers(connections)
      connections.reduce(0) do |memo_i, i|
        memo_i + i.channels.values.reduce(0) { |memo_j, j| memo_j + j.consumers.size }
      end
    end
  end
end
