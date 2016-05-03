require 'cassandra'
require './config/config.rb'
require './lib/cassandra_record.rb'
require './lib/connection.rb'
require './lib/migration.rb'

task :environment do
end

namespace :cequel do
  namespace :keyspace do
    desc 'Initialize Cassandra keyspace'
    task :create => :environment do
      create!
    end

    desc 'Initialize Cassandra keyspace if not exist'
    task :create_if_not_exist => :environment do
      if Cequel::Record.connection.schema.exists?
        puts "Keyspace #{Cequel::Record.connection.name} already exists. Nothing to do."
        next
      end
      create!
    end

    desc 'Drop Cassandra keyspace'
    task :drop => :environment do
      drop!
    end

    desc 'Drop Cassandra keyspace if exist'
    task :drop_if_exist => :environment do
      unless Cequel::Record.connection.schema.exists?
        puts "Keyspace #{Cequel::Record.connection.name} doesn't exist. Nothing to do."
        next
      end
      drop!
    end
  end

  desc "Synchronize all models defined in `app/models' with Cassandra " \
       "database schema"
  task :migrate => :environment do
    migrate
  end

  desc "Create keyspace and tables for all defined models"
  task :init => %w(keyspace:create migrate)


  desc 'Drop keyspace if exists, then create and migrate'
  task :reset => :environment do
    if Cequel::Record.connection.schema.exists?
      drop!
    end
    create!
    migrate
  end

  def create!
    Cequel::Record.connection.schema.create!
    puts "Created keyspace #{Cequel::Record.connection.name}"
  end


  def drop!
    Cequel::Record.connection.schema.drop!
    puts "Dropped keyspace #{Cequel::Record.connection.name}"
  end

  def migrate
    watch_stack = ActiveSupport::Dependencies::WatchStack.new

    migration_table_names = Set[]
    project_root = defined?(Rails) ? Rails.root : Dir.pwd
    models_dir_path = "#{File.expand_path('app/models', project_root)}/"
    model_files = Dir.glob(File.join(models_dir_path, '**', '*.rb'))
    model_files.sort.each do |file|
      watch_namespaces = ["Object"]
      model_file_name = file.sub(/^#{Regexp.escape(models_dir_path)}/, "")
      dirname = File.dirname(model_file_name)
      watch_namespaces << dirname.classify unless dirname == "."
      watch_stack.watch_namespaces(watch_namespaces)
      require_dependency(file)

      new_constants = watch_stack.new_constants
      if new_constants.empty?
        new_constants << model_file_name.sub(/\.rb$/, "").classify
      end

      new_constants.each do |class_name|
        # rubocop:disable HandleExceptions
        begin
          clazz = class_name.constantize
        rescue LoadError, NameError, RuntimeError
        else
          if clazz.is_a?(Class)
            if clazz.ancestors.include?(Cequel::Record) &&
                !migration_table_names.include?(clazz.table_name.to_sym)
              clazz.synchronize_schema
              migration_table_names << clazz.table_name.to_sym
              puts "Synchronized schema for #{class_name}"
            end
          end
        end
        # rubocop:enable HandleExceptions
      end
    end
  end
end

desc 'create db'

task :create do
  m = Migration.new
  cql = <<-KEYSPACE_CQL
            CREATE KEYSPACE IF NOT EXISTS #{Cfg.config['cassandraCluster']['keyspace']}
            WITH replication = {
              'class': 'SimpleStrategy',
              'replication_factor': 1
            }
  KEYSPACE_CQL

  m.execute(cql, false)

  cql = <<-KEYSPACE_CQL
          CREATE TABLE #{Cfg.config['tables']['history_migration']} (
            filename VARCHAR,
            PRIMARY KEY (filename)
          )
  KEYSPACE_CQL

  m.execute(cql)
end

desc 'migrate'
task :migrate do
  migrate_all
end

def migrate_all
  puts 'Running all migrations'
  puts Dir.glob("#{File.dirname(__FILE__)}/data/*.rb").inspect
  Dir.glob("#{File.dirname(__FILE__)}/data/*.rb").sort.each do |migration|
    filename = File.basename(migration).sub('.rb','')
    migration_record = Migration.new.execute("select * from #{Cfg.config['tables']['history_migration']} where filename = '#{filename}'")
    next if migration_record.size > 0
    Migration.new.execute("insert into #{Cfg.config['tables']['history_migration']} (filename) values ('#{filename}')")
    migration_class = filename.sub(/\d+_/,'').split('_').map(&:capitalize).join
    Module.module_eval(File.read(migration))
    print "Running #{migration_class}..."
    migration_class = Module.const_get(migration_class)
    puts 'done'
    migration_class.new.migrate
  end
end
