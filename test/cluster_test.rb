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
             'redis://127.0.0.1:7005'].freeze

    @r = Redis::Cluster.new(nodes)

    100.times { |i| @r.set(i.to_s, "hogehoge#{i}") }
    100.times { |i| assert_equal "hogehoge#{i}", @r.get(i.to_s) }
    assert_equal 'string', @r.type('1')
  end
end
