require 'hdfs-nukumori-chmod_jars'

module Hdfs
  module Nukumori
    module Chmod
      class HdfsClient
        Path = org.apache.hadoop.fs.Path
        FileSystem = org.apache.hadoop.fs.FileSystem
        Configuration = org.apache.hadoop.conf.Configuration
        FsPermission = org.apache.hadoop.fs.permission.FsPermission
        JFile = java.io.File

        def self.get_configuration(conf_files = [])
          Configuration.new.tap do |c|
            conf_files = [] << conf_files if conf_files.is_a?(String)
            
            conf_files.each do |f|
              c.add_resource(JFile.new(f).to_uri.to_url)
            end
          end
        end

        attr_accessor :conf

        def initialize(conf)
          @conf = conf
        end

        def connect
          @fs ||= FileSystem.get(conf)
        end

        def close
          @fs.close rescue nil if @fs
        end

        def ls_r(path)
          path = Path.new(path) unless path.is_a?(Path)
          return @fs.listFiles(path, true) unless block_given?

          @fs.listFiles(path, true).each do |file_status|
            yield(file_status)
          end
        end

        def chmod(path, perm)
          path = Path.new(path) unless path.is_a?(Path)
          perm = FsPermission.new(perm) unless perm.is_a?(FsPermission)

          @fs.set_permission(path, perm)
        end

        def directory?(file_status)
          file_status.is_directory
        end

        def file?(file_status)
          file_status.is_file
        end
      end
    end
  end
end
