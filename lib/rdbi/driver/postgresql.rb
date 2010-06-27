require 'rdbi'
require 'epoxy'
require 'methlab'
require 'pg'

class RDBI::Driver::PostgreSQL < RDBI::Driver
  def initialize( *args )
    super( Database, *args )
  end
end

class RDBI::Driver::PostgreSQL < RDBI::Driver
  class Database < RDBI::Database
    extend MethLab

    attr_accessor :pg_conn

    def initialize( *args )
      super( *args )
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
    end

    def disconnect
      @pg_conn.close
      super
    end

    def transaction( &block )
      if in_transaction?
        raise "[RDBI] Already in transaction (not supported by PostgreSQL)"
      end
      execute 'BEGIN'
      super &block
    end

    def rollback
      if ! in_transaction?
        raise "[RDBI] Cannot rollback when not in a transaction"
      end
      execute 'ROLLBACK'
      super
    end
    def commit
      if ! in_transaction?
        raise "[RDBI] Cannot commit when not in a transaction"
      end
      execute 'COMMIT'
      super
    end

    def new_statement( query )
      Statement.new(query, self)
    end

    def preprocess_query( query, *binds )
      mutex.synchronize { @last_query = query }

      ep = Epoxy.new( query )
      ep.quote { |x| @pg_conn.escape_string( binds[x].to_s ) }
    end

    def schema
      sch = []
      # execute("SELECT name FROM sqlite_master WHERE type='table'").fetch(:all).each do |row|
        # sch << table_schema(row[0])
      # end
      sch
    end

    def table_schema( table_name )
      sch = RDBI::Schema.new( [], [] )
      sch.tables << table_name.to_sym
      @pg_conn.table_info( table_name ) do |hash|
        col = RDBI::Column.new
        col.name       = hash['name'].to_sym
        col.type       = hash['type'].to_sym
        col.ruby_type  = hash['type'].to_sym
        col.nullable   = !(hash['notnull'] == "0")
        sch.columns << col
      end

      sch
    end

    inline(:ping)     { 0 }
  end

  class Statement < RDBI::Statement
    extend MethLab

    attr_accessor :pg_result

    def initialize( query, dbh )
      super( query, dbh )
      # TODO: Choose a better statement name to guarantee uniqueness
      @stmt_name = Time.now.to_f.to_s
      epoxy = Epoxy.new( query )
      query = epoxy.quote { |x| "$#{x+1}" }
      @pg_result = dbh.pg_conn.prepare( @stmt_name, query )
      # @input_type_map initialized in superclass
      @output_type_map = RDBI::Type.create_type_hash( RDBI::Type::Out )
    end

    def new_execution( *binds )
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

      [ ary, this_schema, @output_type_map ]
    end

    def finish
      @pg_result.clear
      super
    end
  end
end
