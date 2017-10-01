require_relative 'cluster/key_slot_converter'

class Redis
  # Redis Cluster client
  #
  # @see https://github.com/antirez/redis-rb-cluster POC implementation
  # @see https://redis.io/topics/cluster-spec Redis Cluster specification
  # @see https://redis.io/topics/cluster-tutorial Redis Cluster tutorial
  #
  # Copyright (C) 2013 Salvatore Sanfilippo <antirez@gmail.com>
  class Cluster
    KEYLESS_COMMANDS = %i[info multi exec slaveof config shutdown].freeze
    RETRY_COUNT = 16
    RETRY_WAIT_SEC = 0.1

    # Create a new client instance.
    #
    # @param [Array<String, Hash>] node_configs list of node addresses
    #   to contact
    # @param [Hash] options same as the `Redis` constractor
    # @return [Redis::Cluster] a new client instance
    def initialize(node_configs, options = {})
      raise ArgumentError, 'Redis Cluster node config must be Array' unless node_configs.is_a?(Array)

      startup_nodes = build_clients_per_node(node_configs, options)
      available_slots = fetch_available_slots_per_node(startup_nodes.values)

      raise CannotConnectError, 'Could not connect to any nodes' if available_slots.nil?

      available_node_addrs = extract_available_node_addrs(available_slots)
      @available_nodes = build_clients_per_node(available_node_addrs, options)
      @slot_node_key_maps = build_slot_node_key_maps(available_slots)
    end

    # Sends `CLUSTER *` command to random node and returns its reply.
    #
    # @see https://redis.io/commands#cluster the cluster command references
    #
    # @param [String, Symbol] command the subcommand
    #   e.g. `:slots`, `:nodes`, `:slaves`, `:info`
    # @return depends on the subcommand
    def cluster(command, *args, &block)
      response = try_cmd(find_node, :cluster, command, *args, &block)
      case command.to_s.downcase
      when 'slots' then cluster_slots(response)
      when 'nodes' then cluster_nodes(response)
      when 'slaves' then cluster_slaves(response)
      when 'info' then cluster_info(response)
      else response
      end
    end

    # Sends `ASKING` command to random node and returns its reply.
    #
    # @see https://redis.io/topics/cluster-spec#ask-redirection ASK redirection
    #
    # @return [String] `OK`
    def asking
      try_cmd(find_node, :synchronize) { |client| client.call(%i[asking]) }
    end

    private

    # Delegates to a instance of random node client and returns its reply.
    #
    # @param [String] method_name the method name
    # @option [true, false] include_private true if private methods needed
    # @return [true, false] depends on a instance of node client implementation
    def respond_to_missing?(method_name, include_private = false)
      find_node.respond_to?(method_name, include_private)
    end

    # Delegates to a instance of random node client and returns its reply.
    #
    # @param [String, Symbol] method_name the method name e.g. `:set`, `:get`
    # @return depends on the method name
    def method_missing(method_name, *args, &block)
      key = extract_key(method_name, *args)
      slot = key.empty? ? nil : KeySlotConverter.convert(key)
      node = find_node(slot)
      return try_cmd(node, method_name, *args, &block) if node.respond_to?(method_name)

      super
    end

    # Finds and returns a instance of node client.
    #
    # @param [nil, Integer] slot the slot number
    # @return [nil, Redis] a instance of node client related to the slot number,
    #   or a instance of random node client if the slot number is nil,
    #   or nil if client not cached slot information.
    def find_node(slot = nil)
      return nil unless instance_variable_defined?(:@available_nodes)
      return @available_nodes.values.sample if slot.nil? || !@slot_node_key_maps.key?(slot)

      node_key = @slot_node_key_maps[slot]
      @available_nodes.fetch(node_key)
    end

    # Sends the command and returns its reply. Redirections may occur.
    #
    # @see https://redis.io/topics/cluster-spec#redirection-and-resharding
    #   Redirection and resharding
    #
    # @param [Redis] node a node client
    # @param [String, Symbol] command the command
    # @option [Integer] :ttl limit of count for retry or redirection
    # @return depends on the command
    def try_cmd(node, command, *args, ttl: RETRY_COUNT, &block)
      ttl -= 1
      node.send(command, *args, &block)
    rescue TimeoutError, CannotConnectError, Errno::ECONNREFUSED, Errno::EACCES => err
      raise err if ttl <= 0
      sleep(RETRY_WAIT_SEC)
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

    # Parse redirection error message
    #   and returns a instance of destination node client.
    #
    # @param [String] err_msg the redirection error message
    # @return [Redis] a instance of destination node client
    def redirection_node(err_msg)
      _, slot, node_key = err_msg.split(' ')
      @slot_node_key_maps[slot.to_i] = node_key
      find_node(slot.to_i)
    end

    # Creates client instances per node.
    #
    # @param [Array<String, Hash>] node_configs list of node addresses
    #   to contact
    # @param [Hash] options same as the `Redis` constractor
    # @return [Hash{String => Redis}] client instances per `'ip:port'`
    def build_clients_per_node(node_configs, options)
      node_configs.map do |config|
        option = to_client_option(config)
        [to_node_key(option), Redis.new(options.merge(option))]
      end.to_h
    end

    # Converts node address into client options.
    #
    # @param [String, Hash] config the node config
    #   e.g. `'redis://127.0.0.1:6379'`, `{ host: '127.0.0.1', port: 6379 }`
    # @return [Hash{Symbol => String, Integer}] converted options
    # @raise [Argumenterror] if config is not a `String` or `Hash`
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

    # Converts client option into key of node address.
    #
    # @param [Hash{Symbol => String, Integer}] option the client option
    #   e.g. `{ url: 'redis://127.0.0.1:6379' }`,
    #   `{ host: '127.0.0.1', port: 6379 }`
    # @return [String] the node key of address e.g. `'127.0.0.1:6379'`
    def to_node_key(option)
      return option[:url].gsub(%r{rediss?://}, '') if option.key?(:url)

      "#{option[:host]}:#{option[:port]}"
    end

    # Fetch cluster slot info on available node.
    #
    # @param [Array<Redis>] startup_nodes list of start-up node clients
    # @return [Hash{String => Range}] slot ranges per key of node address
    def fetch_available_slots_per_node(startup_nodes)
      slot_info = nil

      startup_nodes.each do |node|
        begin
          slot_info = fetch_slot_info(node, ttl: 1)
        rescue CannotConnectError, CommandError
          next
        end

        break
      end

      slot_info
    end

    # Try fetch cluster slot info and converts it into slot range data per node.
    #
    # @param [Redis] node the instance of node client
    # @option [Integer] :ttl limit of count for retry or redirection
    # @return [Hash{String => Range}] slot ranges per key of node address
    def fetch_slot_info(node, ttl: RETRY_COUNT)
      try_cmd(node, :cluster, :slots, ttl: ttl).map do |slot_info|
        first_slot, last_slot = slot_info[0..1]
        ip, port = slot_info[2]
        ["#{ip}:#{port}", (first_slot..last_slot)]
      end.to_h
    end

    # Extracts node addresses from slot info.
    #
    # @param [Hash{String => Range}] available_slots the cluster slot info
    # @return [Array<Hash>] available node addresses
    def extract_available_node_addrs(available_slots)
      available_slots
        .keys
        .map { |k| k.split(':') }
        .map { |k| { host: k[0], port: k[1] } }
    end

    # Creates cache of slot-node mapping.
    #   e.g. `{ 12345 => '127.0.0.1:7000', 67890 => '127.0.0.1:7001' }`
    #
    # @param [Hash{String => Range}] available_slots the cluster slot info
    # @return [Hash{Integer => String}] cache of slot-node mapping
    def build_slot_node_key_maps(available_slots)
      available_slots.each_with_object({}) do |(node_key, slots), m|
        slots.each { |slot| m[slot] = node_key }
      end
    end

    # Extracts command key from arguments.
    #
    # @see https://redis.io/topics/cluster-spec#keys-hash-tags Keys hash tags
    #
    # @param [String] command the command
    # @return [String] the key or blank or hash tag
    def extract_key(command, *args)
      command = command.to_s.downcase.to_sym
      return '' if KEYLESS_COMMANDS.include?(command)

      key = args.first.to_s
      hash_tag = extract_hash_tag(key)
      hash_tag.empty? ? key : hash_tag
    end

    # Extracts hash tag from key.
    #
    # @param [String] key the key
    # @return [String] hash tag
    def extract_hash_tag(key)
      s = key.index('{')
      e = key.index('}', s.to_i + 1)

      return '' if s.nil? || e.nil?

      key[s + 1..e - 1]
    end

    # Deserialize the node info.
    #
    # @param [String] str the node info string data
    # @return [Hash{Symbol => String, Range, nil}] the node info
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

    # Parse `CLUSTER SLOTS` command response raw data.
    #
    # @param [Array<Array>] response raw data
    # @return [Array<Hash>] parsed data
    def cluster_slots(response)
      response.map do |res|
        first_slot, last_slot = res[0..1]
        master = { ip: res[2][0], port: res[2][1], node_id: res[2][2] }
        replicas = res[3..-1].map { |r| { ip: r[0], port: r[1], node_id: r[2] } }
        { start_slot: first_slot, end_slot: last_slot, master: master, replicas: replicas }
      end
    end

    # Parse `CLUSTER NODES` command response raw data.
    #
    # @param [String] response raw data
    # @return [Array<Hash>] parsed data
    def cluster_nodes(response)
      response
        .split(/[\r\n]+/)
        .map { |str| deserialize_node_info(str) }
    end

    # Parse `CLUSTER SLAVES` command response raw data.
    #
    # @param [Array<String>] response raw data
    # @return [Array<Hash>] parsed data
    def cluster_slaves(response)
      response.map { |str| deserialize_node_info(str) }
    end

    # Parse `CLUSTER INFO` command response raw data.
    #
    # @param [String] response raw data
    # @return [Hash{Symbol => String}] parsed data
    def cluster_info(response)
      response
        .split(/[\r\n]+/)
        .map { |str| str.split(':') }
        .map { |arr| [arr.first.to_sym, arr[1]] }
        .to_h
    end
  end
end
