require 'rdbi'
require 'epoxy'
require 'methlab'
require 'pg'

class RDBI::Driver::PostgreSQL < RDBI::Driver
  def initialize(*args)
    super(Database, *args)
  end
end

class RDBI::Driver::PostgreSQL < RDBI::Driver
  class Database < RDBI::Database
    extend MethLab

    attr_accessor :pg_conn

    def initialize(*args)
      super
      self.database_name = @connect_args[:database]
      @pg_conn = PGconn.new(
        @connect_args[:host],
        @connect_args[:port],
        @connect_args[:options],
        @connect_args[:tty],
        @connect_args[:dbname] || @connect_args[:database],
        @connect_args[:user],
        @connect_args[:password] || @connect_args[:auth]
      )
      # @pg_conn.type_translation = false # XXX RDBI should handle this.
    end

    def disconnect
      @pg_conn.close
      super
    end

    def transaction(&block)
      execute 'BEGIN'
      super
      # @pg_conn.transaction do
        # yield @pg_conn
        # super
      # end
    end

    def rollback
      execute 'ROLLBACK'
      super
    end
    def commit
      execute 'COMMIT'
      super
    end

    def new_statement(query)
      Statement.new(query, self)
    end

    def preprocess_query(query, *binds)
      mutex.synchronize { @last_query = query }

      ep = Epoxy.new(query)
      ep.quote { |x| @pg_conn.escape_string( binds[x].to_s ) }
    end

    def schema
      sch = []
      # execute("SELECT name FROM sqlite_master WHERE type='table'").fetch(:all).each do |row|
        # sch << table_schema(row[0])
      # end
      return sch
    end

    def table_schema(table_name)
      sch = RDBI::Schema.new([], [])
      sch.tables << table_name.to_sym
      @pg_conn.table_info(table_name) do |hash|
        col = RDBI::Column.new
        col.name       = hash['name'].to_sym
        col.type       = hash['type'].to_sym
        col.ruby_type  = hash['type'].to_sym
        col.nullable   = !(hash['notnull'] == "0")
        sch.columns << col
      end

      return sch
    end

    inline(:ping)     { 0 }
  end

  class Statement < RDBI::Statement
    extend MethLab

    attr_accessor :pg_result

    def initialize(query, dbh)
      super
      @dbh = dbh
      # TODO: Choose a better statement name to guarantee uniqueness
      @stmt_name = Time.now.to_f.to_s
      epoxy = Epoxy.new( query )
      @query = query = epoxy.quote { |x| "$#{x+1}" }
      @pg_result = dbh.pg_conn.prepare( @stmt_name, query )
      @input_type_map  = RDBI::Type.create_type_hash(RDBI::Type::In)
      @output_type_map = RDBI::Type.create_type_hash(RDBI::Type::Out)
      # @output_type_map[ :timestamp_with_time_zone ] = RDBI::Type.filterlist(
        # TypeLib::Canned.build_strptime_filter( "%Y-%m-%d %H:%M:%S %z" )
      # )
      # @output_type_map[ :timestamp_without_time_zone ] = RDBI::Type.filterlist(
        # TypeLib::Canned.build_strptime_filter( "%Y-%m-%d %H:%M:%S" )
      # )
      # @output_type_map[ :timestamp ] = @output_type_map[ :timestamp_without_time_zone ]
    end

    def new_execution(*binds)
      pg_result = @dbh.pg_conn.exec_prepared( @stmt_name, binds )
      ary = pg_result.to_a.map { |h| h.values }

      columns = []
      (0...pg_result.num_fields).each do |i|
        c = RDBI::Column.new
        c.name = pg_result.fname( i )
        c.type = @dbh.pg_conn.exec(
          "SELECT format_type( #{ pg_result.ftype(i) }, #{ pg_result.fmod(i) } )"
        )[ 0 ].values[ 0 ]
        if c.type == 'timestamp without time zone'
          ary.each do |row|
            row[ i ] << DateTime.now.strftime( " %z" )
          end
        end
        # This could be faster without a regexp
        if c.type.start_with? 'timestamp'
          c.ruby_type = 'timestamp'.to_sym
        else
          c.ruby_type = c.type.to_sym
        end
        columns << c
      end

      this_schema = RDBI::Schema.new
      this_schema.columns = columns

      pg_result.clear

      return ary, this_schema, @output_type_map
    end

    def finish
      @pg_result.clear
      super
    end
  end
end
