require_relative 'helper'

# bundle exec ruby -w -Itest test/cluster_test.rb
class TestCluster < Test::Unit::TestCase
  include Helper::Cluster

  def test_set_and_get
    nodes = ['redis://127.0.0.1:7000',
             'redis://127.0.0.1:7001',
             { host: '127.0.0.1', port: '7002' },
             { host: '127.0.0.1', port: 7003 },
             'redis://127.0.0.1:7004',
             'redis://127.0.0.1:7005']

    @r = Redis::Cluster.new(nodes)

    100.times { |i| @r.set(i.to_s, "hogehoge#{i}") }
    100.times { |i| assert_equal "hogehoge#{i}", @r.get(i.to_s) }
    assert_equal 'string', @r.type('1')
  end

  def test_not_exist_nodes
    nodes = ['redis://127.0.0.1:7006']

    @r = Redis::Cluster.new(nodes)

    assert_raise(Redis::CannotConnectError) do
      @r.set('hogehoge', 1)
    end
  end

  def test_http_scheme
    nodes = ['http://127.0.0.1:80']

    assert_raise(ArgumentError, "invalid uri scheme 'http'") do
      @r = Redis::Cluster.new(nodes)
    end
  end

  def test_unix_scheme
    nodes = ['unix://tmp/redis.sock']

    assert_nothing_raised(ArgumentError, "invalid uri scheme 'unix'") do
      @r = Redis::Cluster.new(nodes)
    end
  end

  def test_blank_config
    nodes = ['']

    assert_raise(ArgumentError, "invalid uri scheme ''") do
      @r = Redis::Cluster.new(nodes)
    end
  end

  def test_bool_config
    nodes = [true]

    assert_raise(ArgumentError, "invalid uri scheme ''") do
      @r = Redis::Cluster.new(nodes)
    end
  end

  def test_nil_config
    nodes = [nil]

    assert_raise(ArgumentError, "invalid uri scheme ''") do
      @r = Redis::Cluster.new(nodes)
    end
  end

  def test_array_config
    nodes = [[]]

    assert_raise(ArgumentError, "invalid uri scheme ''") do
      @r = Redis::Cluster.new(nodes)
    end
  end

  def test_hash_config
    nodes = [{}]

    assert_raise(KeyError, 'key not found: :host') do
      @r = Redis::Cluster.new(nodes)
    end
  end

  def test_object_config
    nodes = [Object.new]

    assert_raise(ArgumentError, 'Redis Cluster node config must includes String or Hash') do
      @r = Redis::Cluster.new(nodes)
    end
  end

  def test_not_array_config
    nodes = :not_array

    assert_raise(ArgumentError, 'Redis Cluster node config must be Array') do
      @r = Redis::Cluster.new(nodes)
    end
  end
end
