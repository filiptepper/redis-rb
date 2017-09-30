require_relative 'helper'

# ruby -w -Itest test/cluster_test.rb
class TestCluster < Test::Unit::TestCase
  include Helper::Cluster

  def test_extract_hash_tag
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    redis = Redis::Cluster.new(nodes)

    assert_equal 'user1000', redis.send(:extract_hash_tag, '{user1000}.following')
    assert_equal 'user1000', redis.send(:extract_hash_tag, '{user1000}.followers')
    assert_equal '', redis.send(:extract_hash_tag, 'foo{}{bar}')
    assert_equal '{bar', redis.send(:extract_hash_tag, 'foo{{bar}}zap')
    assert_equal 'bar', redis.send(:extract_hash_tag, 'foo{bar}{zap}')
  end

  def test_cluster_slots
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    redis = Redis::Cluster.new(nodes)
    slots = redis.cluster('slots')

    assert_equal 3, slots.length
    assert_equal true, slots.first.key?(:start_slot)
    assert_equal true, slots.first.key?(:end_slot)
    assert_equal true, slots.first.key?(:master)
    assert_equal true, slots.first.fetch(:master).key?(:ip)
    assert_equal true, slots.first.fetch(:master).key?(:port)
    assert_equal true, slots.first.fetch(:master).key?(:node_id)
    assert_equal true, slots.first.key?(:replicas)
    assert_equal true, slots.first.fetch(:replicas).is_a?(Array)
    assert_equal true, slots.first.fetch(:replicas).first.key?(:ip)
    assert_equal true, slots.first.fetch(:replicas).first.key?(:port)
    assert_equal true, slots.first.fetch(:replicas).first.key?(:node_id)
  end

  def test_cluster_keyslot
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    redis = Redis::Cluster.new(nodes)

    assert_equal Redis::Cluster::KeySlotConverter.convert('hogehoge'), redis.cluster('keyslot', 'hogehoge')
    assert_equal Redis::Cluster::KeySlotConverter.convert('12345'), redis.cluster('keyslot', '12345')
    assert_equal Redis::Cluster::KeySlotConverter.convert('foo'), redis.cluster('keyslot', 'boo{foo}woo')
    assert_equal Redis::Cluster::KeySlotConverter.convert('antirez.is.cool'), redis.cluster('keyslot', 'antirez.is.cool')
  end

  def test_cluster_nodes
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    redis = Redis::Cluster.new(nodes)
    cluster_nodes = redis.cluster('nodes')

    assert_equal 6, cluster_nodes.length
    assert_equal true, cluster_nodes.first.key?(:node_id)
    assert_equal true, cluster_nodes.first.key?(:ip_port)
    assert_equal true, cluster_nodes.first.key?(:flags)
    assert_equal true, cluster_nodes.first.key?(:master_node_id)
    assert_equal true, cluster_nodes.first.key?(:ping_sent)
    assert_equal true, cluster_nodes.first.key?(:pong_recv)
    assert_equal true, cluster_nodes.first.key?(:config_epoch)
    assert_equal true, cluster_nodes.first.key?(:link_state)
    assert_equal true, cluster_nodes.first.key?(:slots)
  end

  def test_cluster_slaves
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    redis = Redis::Cluster.new(nodes)
    cluster_nodes = redis.cluster('nodes')

    sample_master_node_id = cluster_nodes.find { |n| n.fetch(:master_node_id) == '-' }.fetch(:node_id)
    sample_slave_node_id = cluster_nodes.find { |n| n.fetch(:master_node_id) != '-' }.fetch(:node_id)

    assert_equal 'slave', redis.cluster('slaves', sample_master_node_id).first.fetch(:flags).first
    assert_raise(Redis::CommandError, 'ERR The specified node is not a master') do
      redis.cluster('slaves', sample_slave_node_id)
    end
  end

  def test_cluster_info
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    redis = Redis::Cluster.new(nodes)

    assert_equal '3', redis.cluster('info').fetch(:cluster_size)
  end

  def test_asking
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    redis = Redis::Cluster.new(nodes)

    assert_equal 'OK', redis.asking
  end

  def test_client_works_even_if_so_many_dead_nodes_existed
    nodes = (6001..7005).map { |port| "redis://127.0.0.1:#{port}" }

    assert_nothing_raised do
      redis = Redis::Cluster.new(nodes)
      redis.ping('Hello world')
    end
  end

  def test_well_known_commands_work
    nodes = ['redis://127.0.0.1:7000',
             'redis://127.0.0.1:7001',
             { host: '127.0.0.1', port: '7002' },
             { 'host' => '127.0.0.1', port: 7003 },
             'redis://127.0.0.1:7004',
             'redis://127.0.0.1:7005']

    redis = Redis::Cluster.new(nodes)

    100.times { |i| redis.set(i.to_s, "hogehoge#{i}") }
    100.times { |i| assert_equal "hogehoge#{i}", redis.get(i.to_s) }
    assert_equal '1', redis.info['cluster_enabled']
  end

  def test_client_respond_to_commands
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    redis = Redis::Cluster.new(nodes)

    assert_equal true, redis.respond_to?(:set)
    assert_equal true, redis.respond_to?('set')
    assert_equal true, redis.respond_to?(:get)
    assert_equal true, redis.respond_to?('get')
    assert_equal true, redis.respond_to?(:cluster)
    assert_equal true, redis.respond_to?(:asking)
    assert_equal false, redis.respond_to?(:unknown_method)
  end

  def test_unknown_command_does_not_work
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    redis = Redis::Cluster.new(nodes)

    assert_raise(NoMethodError) do
      redis.not_yet_implemented_command('boo', 'foo')
    end
  end

  def test_client_does_not_accept_db_specified_url
    nodes = ['redis://127.0.0.1:7000/1/namespace']

    assert_raise(Redis::CommandError, 'ERR SELECT is not allowed in cluster mode') do
      Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_unconnectable_node_url_only
    nodes = ['redis://127.0.0.1:7006']

    assert_raise(Redis::CannotConnectError, 'Could not connect to any nodes') do
      Redis::Cluster.new(nodes)
    end
  end

  def test_client_accept_unconnectable_node_url_included
    nodes = ['redis://127.0.0.1:7000', 'redis://127.0.0.1:7006']

    assert_nothing_raised(Redis::CannotConnectError, 'Could not connect to any nodes') do
      Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_http_scheme_url
    nodes = ['http://127.0.0.1:80']

    assert_raise(ArgumentError, "invalid uri scheme 'http'") do
      Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_blank_included_config
    nodes = ['']

    assert_raise(ArgumentError, "invalid uri scheme ''") do
      Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_bool_included_config
    nodes = [true]

    assert_raise(ArgumentError, "invalid uri scheme ''") do
      Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_nil_included_config
    nodes = [nil]

    assert_raise(ArgumentError, "invalid uri scheme ''") do
      Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_array_included_config
    nodes = [[]]

    assert_raise(ArgumentError, "invalid uri scheme ''") do
      Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_empty_hash_included_config
    nodes = [{}]

    assert_raise(KeyError, 'key not found: :host') do
      Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_object_included_config
    nodes = [Object.new]

    assert_raise(ArgumentError, 'Redis Cluster node config must includes String or Hash') do
      Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_not_array_config
    nodes = :not_array

    assert_raise(ArgumentError, 'Redis Cluster node config must be Array') do
      Redis::Cluster.new(nodes)
    end
  end
end
