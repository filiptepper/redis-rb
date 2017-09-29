require_relative 'cluster/key_slot_converter'

class Redis
  # Copyright (C) 2013 Salvatore Sanfilippo <antirez@gmail.com>
  # https://github.com/antirez/redis-rb-cluster
  class Cluster
    REQUEST_TTL = 16
    REQUEST_RETRY_SLEEP = 0.1

    def initialize(node_configs, options = {})
      raise ArgumentError, 'Redis Cluster node config must be Array' unless node_configs.is_a?(Array)

      startup_nodes = build_clients_per_node(node_configs, options)
      available_slots = fetch_available_slots_per_node(startup_nodes.values)

      raise CannotConnectError, 'Could not connect to any nodes' if available_slots.nil?

      available_node_urls = available_slots.keys.map { |ip_port| "redis://#{ip_port}" }
      @available_nodes = build_clients_per_node(available_node_urls, options)
      @slot_node_key_maps = build_slot_node_key_maps(available_slots)
    end

    def cluster(command, *args)
      response = try_cmd(find_node, :cluster, command, *args)
      case command.to_s.downcase
      when 'slots' then cluster_slots(response)
      when 'nodes' then cluster_nodes(response)
      when 'slaves' then cluster_slaves(response)
      when 'info' then cluster_info(response)
      else response
      end
    end

    def asking
      try_cmd(find_node, :synchronize) { |client| client.call(%i[asking]) }
    end

    private

    def respond_to_missing?(method_name, include_private = false)
      find_node.respond_to?(method_name, include_private)
    end

    def method_missing(method_name, *args, &block)
      key = extract_key(args)
      slot = KeySlotConverter.convert(key)
      node = find_node(slot)
      return try_cmd(node, method_name, *args, &block) if node.respond_to?(method_name)
      super
    end

    def find_node(slot = nil)
      return nil unless instance_variable_defined?(:@available_nodes)
      return @available_nodes.values.sample if slot.nil? || !@slot_node_key_maps.key?(slot)

      node_key = @slot_node_key_maps[slot]
      @available_nodes.fetch(node_key)
    end

    def try_cmd(node, command, *args, ttl: REQUEST_TTL, &block)
      ttl -= 1
      node.send(command, *args, &block)
    rescue TimeoutError, CannotConnectError, Errno::ECONNREFUSED, Errno::EACCES => err
      raise err if ttl <= 0
      sleep(REQUEST_RETRY_SLEEP)
      node = find_node || node
      retry
    rescue CommandError => err
      if err.message.start_with?('MOVED')
        redirection_node(err.message).send(command, *args, &block)
      elsif err.message.start_with?('ASK')
        raise err if ttl <= 0
        asking
        retry
      else
        raise err
      end
    end

    def redirection_node(err_msg)
      _, slot, node_key = err_msg.split(' ')
      @slot_node_key_maps[slot.to_i] = node_key
      find_node(slot.to_i)
    end

    def build_clients_per_node(node_configs, options)
      node_configs.map do |config|
        option = to_client_option(config)
        [to_node_key(option), Redis.new(options.merge(option))]
      end.to_h
    end

    def to_client_option(config)
      if config.is_a?(String)
        { url: config }
      elsif config.is_a?(Hash)
        config = config.map { |k, v| [k.to_sym, v] }.to_h
        { host: config.fetch(:host), port: config.fetch(:port) }
      else
        raise ArgumentError, 'Redis Cluster node config must includes String or Hash'
      end
    end

    def to_node_key(option)
      return option[:url].gsub(%r{rediss?://}, '') if option.key?(:url)

      "#{option[:host]}:#{option[:port]}"
    end

    def fetch_available_slots_per_node(startup_nodes)
      slot_info = nil

      startup_nodes.each do |node|
        begin
          slot_info = fetch_slot_info(node)
        rescue CannotConnectError
          next
        end

        break
      end

      slot_info
    end

    def fetch_slot_info(node)
      try_cmd(node, :cluster, :slots).map do |slot_info|
        first_slot, last_slot = slot_info[0..1]
        ip, port = slot_info[2]
        ["#{ip}:#{port}", (first_slot..last_slot)]
      end.to_h
    end

    def build_slot_node_key_maps(available_slots)
      available_slots.each_with_object({}) do |(node_key, slots), m|
        slots.each { |slot| m[slot] = node_key }
      end
    end

    def extract_key(args)
      key = args.first.to_s
      hash_tag = extract_hash_tag(key)
      hash_tag.empty? ? key : hash_tag
    end

    def extract_hash_tag(key)
      s = key.index('{')
      e = key.index('}', s.to_i + 1)

      return '' if s.nil? || e.nil?

      key[s + 1..e - 1]
    end

    def deserialize_node_info(str)
      arr = str.split(' ')
      {
        node_id: arr[0],
        ip_port: arr[1],
        flags: arr[2].split(','),
        master_node_id: arr[3],
        ping_sent: arr[4],
        pong_recv: arr[5],
        config_epoch: arr[6],
        link_state: arr[7],
        slots: arr[8].nil? ? nil : Range.new(*arr[8].split('-'))
      }
    end

    def cluster_slots(response)
      response.map do |res|
        first_slot, last_slot = res[0..1]
        master = { ip: res[2][0], port: res[2][1], node_id: res[2][2] }
        replicas = res[3..-1].map { |r| { ip: r[0], port: r[1], node_id: r[2] } }
        { start_slot: first_slot, end_slot: last_slot, master: master, replicas: replicas }
      end
    end

    def cluster_nodes(response)
      response
        .split(/[\r\n]+/)
        .map { |str| deserialize_node_info(str) }
    end

    def cluster_slaves(response)
      response.map { |str| deserialize_node_info(str) }
    end

    def cluster_info(response)
      response
        .split(/[\r\n]+/)
        .map { |str| str.split(':') }
        .map { |arr| [arr.first.to_sym, arr[1]] }
        .to_h
    end
  end
end
