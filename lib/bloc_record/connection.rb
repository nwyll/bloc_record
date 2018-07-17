require 'sqlite3'
require 'pg'

module Connection
  def connection
    if @db == :sqlite3
      database_connection = SQLite3::Database.new(BlocRecord.database_filename)
    elsif @db == :pg
      database_connection = PG::Connection.new(:dbname => BlocRecord.database_filename)
    end

    @connection ||= database_connection
    # @connection ||= SQLite3::Database.new(BlocRecord.database_filename)
  end

  def execute(sql)
    if @db == :sqlite3
      @connection.execute(sql)
    elsif @db == :pg
      @connection.exec(sql)
    end
  end

  def get_first_row(sql)
    if @db == :sqlite3
      @connection.get_first_row(sql)
    elsif @db == :pg
      @connection.exec(sql)[0]
    end
  end

end
