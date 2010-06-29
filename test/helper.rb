require 'rubygems'
gem 'test-unit'
require 'test/unit'
require 'fileutils'

$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'rdbi'
require 'rdbi/driver/postgresql'

class Test::Unit::TestCase

  SQL = [
    'DROP TABLE IF EXISTS foo',
    'DROP TABLE IF EXISTS bar',
    'DROP TABLE IF EXISTS time_test',
    'DROP TABLE IF EXISTS ordinals',
    'create table foo (bar integer)',
    'create table bar (foo varchar, bar integer)',
    'create table time_test (my_date timestamp)',
    'CREATE TABLE ordinals ( id SERIAL PRIMARY KEY, s VARCHAR )',
    "INSERT INTO ordinals ( s ) VALUES ( 'first' )",
    "INSERT INTO ordinals ( s ) VALUES ( 'second' )",
    "INSERT INTO ordinals ( s ) VALUES ( 'third' )",
  ]

  def new_database
    RDBI.connect( :PostgreSQL, :database => 'rdbi', :user => 'rdbi' )
  end

  def init_database
    dbh = new_database
    SQL.each { |query| dbh.execute(query) }
    return dbh
  end
end
