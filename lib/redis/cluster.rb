require_relative 'cluster/key_slot_converter'

class Redis
  # Copyright (C) 2013 Salvatore Sanfilippo <antirez@gmail.com>
  # https://github.com/antirez/redis-rb-cluster
  class Cluster
    def initialize(node_configs, options = {})
      @slot_node_maps = {}
      raise ArgumentError, 'Redis Cluster node config must be Array' unless node_configs.is_a?(Array)
      @cluster = node_configs.map do |config|
        option = to_client_option(config)
        [to_node_key(option), Redis.new(options.merge(option))]
      end.to_h
    end

    private

    def respond_to_missing?(method_name, _include_private = false)
      @cluster.values.first.respond_to?(method_name)
    end

    def method_missing(method_name, *args)
      key = extract_key(args)
      slot = KeySlotConverter.convert(key)
      node = select_node(slot)
      return try_cmd(node, method_name, *args) if node.respond_to?(method_name)
      super
    end

    def to_client_option(config)
      if config.is_a?(String)
        { url: config }
      elsif config.is_a?(Hash)
        { host: config.fetch(:host), port: config.fetch(:port) }
      else
        raise ArgumentError, 'Redis Cluster node config must includes String or Hash'
      end
    end

    def to_node_key(option)
      if option.key?(:url)
        option[:url].gsub('redis://', '')
      else
        "#{option[:host]}:#{option[:port]}"
      end
    end

    def extract_key(args)
      key = args.first.to_s
      return key[1..-2] if key.start_with?('{') && key.end_with?('}')
      key
    end

    def select_node(slot)
      if @slot_node_maps.key?(slot)
        node_key = @slot_node_maps[slot]
        @cluster.fetch(node_key)
      else
        nodes = @cluster.values
        nodes[rand(nodes.length - 1)]
      end
    end

    def try_cmd(node, command, *args)
      node.send(command, *args)
    rescue Redis::CommandError => err
      msg = err.message
      return destination_node(msg).send(command, *args) if redirection_needed?(msg)
      raise err
    end

    def redirection_needed?(err_msg)
      err_msg.start_with?('MOVED')
    end

    def destination_node(err_msg)
      _, slot, host_port = err_msg.split(' ')
      @slot_node_maps[slot.to_i] = host_port
      @cluster.fetch(host_port)
    end
  end
end
