require 'optparse'
require 'logger'
require 'hdfs_client'
require 'error'

module Hdfs
  module Nukumori
    module Chmod
      class Cli
        attr_accessor :path, :conf

        def initialize(argv = ARGV)
          opt = OptionParser.new
          opt.on('-c XML') {|v| conf = v}
          opt.on('-p HDFS_PATH') {|v| path = v}
          opt.parse!(argv)

          validate!
        end

        def validate!
          raise Error, "conf is required." unless conf
          raise Error, "path is required." unless path
        end

        def logger
          @logger ||= ::Logger.new(STDOUT).tap do |l|
            l.level = ::Logge::DEBUG
          end
        end

        def run
          _conf = HdfsClient.get_configuration(conf)
          cli = HdfsClient.new(_conf)

          cli.ls_r(path) do |fst|
            if cli.directory?(fst)
              logger.info("hadoop fs -chmod 755 #{fst.path}; # #{fst}")
              cli.chmod(fst.path, "0755")
            elsif cli.file?(fst)
              logger.info("hadoop fs -chmod 644 #{fst.path}; # #{fst}")
              cli.chmod(fst.path, "0644")
            else
              raise Error, "unknown file status."
            end
          end
        end
      end
    end
  end
end
