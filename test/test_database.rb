require 'helper'

class TestDatabase < Test::Unit::TestCase

  attr_accessor :dbh

  def teardown
    @dbh.disconnect if (@dbh and @dbh.connected?)
  end

  def test_01_connect
    self.dbh = new_database
    assert(dbh)
    assert_kind_of(RDBI::Driver::PostgreSQL::Database, dbh)
    assert_kind_of(RDBI::Database, dbh)
    assert_equal(dbh.database_name, "rdbi")
    dbh.disconnect
    assert(!dbh.connected?)
  end

  def test_02_ping
    assert_equal(0, RDBI.ping(:PostgreSQL, :database => "rdbi"))
    self.dbh = new_database
    assert_equal(0, dbh.ping)
  end

  def test_03_execute
    self.dbh = init_database
    res = dbh.execute("insert into foo (bar) values (?)", 1)
    assert(res)
    assert_kind_of(RDBI::Result, res)

    res = dbh.execute("select * from foo")
    assert(res)
    assert_kind_of(RDBI::Result, res)
    assert_equal([[1]], res.fetch(:all))
  end

  def test_04_prepare
    self.dbh = init_database

    sth = dbh.prepare("insert into foo (bar) values (?)")
    assert(sth)
    assert_kind_of(RDBI::Statement, sth)
    assert_respond_to(sth, :execute)

    5.times { sth.execute(1) }

    assert_equal(dbh.last_statement.object_id, sth.object_id)

    sth2 = dbh.prepare("select * from foo")
    assert(sth)
    assert_kind_of(RDBI::Statement, sth)
    assert_respond_to(sth, :execute)

    res = sth2.execute
    assert(res)
    assert_kind_of(RDBI::Result, res)
    assert_equal([[1]] * 5, res.fetch(:all))

    sth.execute(1)

    res = sth2.execute
    assert(res)
    assert_kind_of(RDBI::Result, res)
    assert_equal([[1]] * 6, res.fetch(:all))

    sth.finish
    sth2.finish
  end

  def test_05_transaction
    self.dbh = init_database

    dbh.transaction do
      assert(dbh.in_transaction?)
      5.times { dbh.execute("insert into foo (bar) values (?)", 1) }
      dbh.rollback
      assert(!dbh.in_transaction?)
    end

    assert(!dbh.in_transaction?)

    assert_equal([], dbh.execute("select * from foo").fetch(:all))

    dbh.transaction do
      assert(dbh.in_transaction?)
      5.times { dbh.execute("insert into foo (bar) values (?)", 1) }
      assert_equal([[1]] * 5, dbh.execute("select * from foo").fetch(:all))
      dbh.commit
      assert(!dbh.in_transaction?)
    end

    assert(!dbh.in_transaction?)

    assert_equal([[1]] * 5, dbh.execute("select * from foo").fetch(:all))

    dbh.transaction do
      assert dbh.in_transaction?
      assert_raises do
        dbh.transaction do
        end
      end
    end
  end

  def test_06_preprocess_query
    self.dbh = init_database
    assert_equal(
      "insert into foo (bar) values (1)",
      dbh.preprocess_query("insert into foo (bar) values (?)", 1)
    )
  end

  def test_07_schema
    self.dbh = init_database

    dbh.execute( 'DROP TABLE IF EXISTS bar' )
    dbh.execute("create table bar (foo varchar, bar integer)")
    dbh.execute("insert into bar (foo, bar) values (?, ?)", "foo", 1)
    res = dbh.execute("select * from bar")

    assert(res)
    assert(res.schema)
    assert_kind_of(RDBI::Schema, res.schema)
    assert(res.schema.columns)
    res.schema.columns.each { |x| assert_kind_of(RDBI::Column, x) }
  end

  def test_08_datetime
    self.dbh = init_database

    dt = DateTime.now
    dbh.execute('insert into time_test (my_date) values (?)', dt)
    dt2 = dbh.execute('select * from time_test limit 1').fetch(1)[0][0]

    assert_kind_of(DateTime, dt2)
    assert_equal(dt2.to_s, dt.to_s)
  end

  def test_09_basic_schema
    self.dbh = init_database
    assert_respond_to(dbh, :schema)
    assert_respond_to(dbh, :table_schema)
    schema = dbh.schema.sort_by { |x| x.tables[0].to_s }

    tables = [:foo, :time_test]
    columns = {
      :foo => { :bar => :integer },
      :time_test => { :my_date => :timestamp }
    }

    schema.each_with_index do |sch, x|
      assert_kind_of(RDBI::Schema, sch)
      assert_equal(sch.tables[0], tables[x])

      sch.columns.each do |col|
        assert_kind_of(RDBI::Column, col)
        assert_equal(columns[tables[x]][col.name], col.type)
      end
    end
  end
end
