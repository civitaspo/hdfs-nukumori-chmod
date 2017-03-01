require 'optparse'
require 'logger'
require_relative 'hdfs_client'
require_relative 'error'

module Hdfs
  module Nukumori
    module Chmod
      class Cli
        attr_accessor :path, :conf

        def initialize(argv = ARGV)
          opt = OptionParser.new
          opt.on('-c CONF_PATH') {|v| @conf = v.split(",")}
          opt.on('-p HDFS_PATH') {|v| @path = v}
          opt.parse!(argv)

          validate!
        end

        def validate!
          raise Error, "conf is required." if conf.nil? or conf.empty?
          raise Error, "path is required." unless path
        end

        def logger
          @logger ||= ::Logger.new(STDOUT).tap do |l|
            l.level = ::Logger::DEBUG
          end
        end

        def run
          _conf = HdfsClient.get_configuration(conf)
          cli = HdfsClient.new(_conf)
          cli.connect

          cli.ls_r(path) do |fst|
            if cli.directory?(fst)
              logger.info("hadoop fs -chmod 755 #{fst.path}; # #{fst}")
              cli.chmod(fst.path, "755")
            elsif cli.file?(fst)
              logger.info("hadoop fs -chmod 644 #{fst.path}; # #{fst}")
              cli.chmod(fst.path, "644")
            else
              raise Error, "unknown file status."
            end
          end

        ensure
          cli.close rescue nil
        end
      end
    end
  end
end
