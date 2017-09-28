require_relative 'helper'

# ruby -w -Itest test/cluster_test.rb
class TestCluster < Test::Unit::TestCase
  include Helper::Cluster

  def test_well_known_commands_work
    nodes = ['redis://127.0.0.1:7000',
             'redis://127.0.0.1:7001',
             { host: '127.0.0.1', port: '7002' },
             { 'host' => '127.0.0.1', port: 7003 },
             'redis://127.0.0.1:7004',
             'redis://127.0.0.1:7005']

    @r = Redis::Cluster.new(nodes)

    100.times { |i| @r.set(i.to_s, "hogehoge#{i}") }
    100.times { |i| assert_equal "hogehoge#{i}", @r.get(i.to_s) }
    assert_equal '1', @r.info['cluster_enabled']
  end

  def test_client_respond_to_commands
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    @r = Redis::Cluster.new(nodes)

    assert_equal true, @r.respond_to?(:set)
    assert_equal true, @r.respond_to?('set')
    assert_equal true, @r.respond_to?(:get)
    assert_equal true, @r.respond_to?('get')
    assert_equal false, @r.respond_to?(:unknown_method)
  end

  def test_unknown_command_does_not_work
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    @r = Redis::Cluster.new(nodes)

    assert_raise(NoMethodError) do
      @r.not_yet_implemented_command('boo', 'foo')
    end
  end

  def test_client_does_not_accept_db_specified_url
    nodes = ['redis://127.0.0.1:7000/1/namespace']

    @r = Redis::Cluster.new(nodes)

    assert_raise(Redis::CommandError, 'ERR SELECT is not allowed in cluster mode') do
      @r.set('key', 'value')
    end
  end

  def test_client_does_not_accept_unconnectable_node_url
    nodes = ['redis://127.0.0.1:7006']

    @r = Redis::Cluster.new(nodes)

    assert_raise(Redis::CannotConnectError) do
      @r.set('hogehoge', 1)
    end
  end

  def test_client_does_not_accept_http_scheme_url
    nodes = ['http://127.0.0.1:80']

    assert_raise(ArgumentError, "invalid uri scheme 'http'") do
      @r = Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_unix_scheme_url
    nodes = ['unix://tmp/redis.sock']

    assert_nothing_raised(ArgumentError, "invalid uri scheme 'unix'") do
      @r = Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_blank_included_config
    nodes = ['']

    assert_raise(ArgumentError, "invalid uri scheme ''") do
      @r = Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_bool_included_config
    nodes = [true]

    assert_raise(ArgumentError, "invalid uri scheme ''") do
      @r = Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_nil_included_config
    nodes = [nil]

    assert_raise(ArgumentError, "invalid uri scheme ''") do
      @r = Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_array_included_config
    nodes = [[]]

    assert_raise(ArgumentError, "invalid uri scheme ''") do
      @r = Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_empty_hash_included_config
    nodes = [{}]

    assert_raise(KeyError, 'key not found: :host') do
      @r = Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_object_included_config
    nodes = [Object.new]

    assert_raise(ArgumentError, 'Redis Cluster node config must includes String or Hash') do
      @r = Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_not_array_config
    nodes = :not_array

    assert_raise(ArgumentError, 'Redis Cluster node config must be Array') do
      @r = Redis::Cluster.new(nodes)
    end
  end
end
