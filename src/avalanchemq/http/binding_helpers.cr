require "../amqp"
require "../exchange"

module AvalancheMQ
  module HTTP
    module BindingHelpers
      private def bindings(vhost)
        vhost.exchanges.each_value.flat_map do |e|
          e.bindings_details
        end
      end

      private def binding_for_props(context, source, destination : Queue, props)
        binding = source.queue_bindings.find do |k, v|
          v.includes?(destination) && BindingDetails.hash_key(k) == props
        end
        unless binding
          not_found(context, "Binding '#{props}' on exchange '#{source.name}' -> queue '#{destination.name}' does not exist")
        end
        binding
      end

      private def binding_for_props(context, source, destination : Exchange, props)
        binding = source.exchange_bindings.find do |k, v|
          v.includes?(destination) && BindingDetails.hash_key(k) == props
        end
        unless binding
          not_found(context, "Binding '#{props}' on exchange '#{source.name}' -> exchange '#{destination.name}' does not exist")
        end
        binding
      end

      private def unbind_prop(source : Queue | Exchange, destination : Queue | Exchange, key : String)
        key = source.bindings.keys.find do |k|
          BindingDetails.hash_key(k) == key
        end
        source.unbind(destination, key[0], key[1]) if key
      end
    end
  end
end
