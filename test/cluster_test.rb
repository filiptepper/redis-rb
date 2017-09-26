require_relative 'helper'
require_relative '../lib/redis/cluster'

# ruby -w -Itest test/cluster_test.rb
class TestCluster < Test::Unit::TestCase
  def test_set_and_get
    @r = Redis::Cluster.new(['redis://127.0.0.1:7000', 'redis://127.0.0.1:7001',
                             'redis://127.0.0.1:7002', 'redis://127.0.0.1:7003',
                             'redis://127.0.0.1:7004', 'redis://127.0.0.1:7005'])

    100.times { |i| @r.set(i.to_s, "hogehoge#{i}") }

    100.times { |i| assert_equal "hogehoge#{i}", @r.get(i.to_s) }
    assert_equal 'string', @r.type('1')
  end
end
