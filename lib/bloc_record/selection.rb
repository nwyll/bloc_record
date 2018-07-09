require 'sqlite3'

module Selection
  def find(*ids)
    # check that ids are numbers > 0
    raise raise ArgumentError, 'Ids given must be values greater than zero.' unless ids.all? { |id| id.is_a? Integer && id > 0 }

    if ids.length == 1
      find_one(ids.first)
    else
      rows = connection.execute <<-SQL
        SELECT #{columns.join ","} FROM #{table}
        WHERE id IN (#{ids.join(",")});
      SQL

      rows_to_array(rows)
    end
  end

  def find_one(id)
    #checking that id is a num > 0
    raise ArgumentError, 'Id given must be a value greater than zero.' unless (id.is_a? Integer && x > 0)

    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE id = #{id};
    SQL

    init_object_from_row(row)
  end

  def find_by(attribute, value)
    #raise error if the given attribute is not listed in the table schema
    raise "#{attribute} is not part of #{table}" unless columns.include?(attribute)

    rows = connection.execute <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE #{attribute} = #{BlocRecord::Utility.sql_strings(value)};
    SQL

    rows_to_array(rows)
  end

  def find_each(start=nil, batch_size=100)
    #start and batch_size should be integers > 0
    raise ArgumentError, 'Value given must be greater than zero.' unless (start.is_a? Integer && x > 0)
    raise ArgumentError, 'Value given must be greater than zero.' unless (batch_size.is_a? Integer && x > 0)

    if block_given?
      find_in_batches(start: start, batch_size: batch_size) do |records|
        records.each { |record| yield record }
      end
    end
  end

  def find_in_batches(start=nil, batch_size=100)
    #start and batch_size should be integers > 0
    raise ArgumentError, 'Value given must be greater than zero.' unless (start.is_a? Integer && x > 0)
    raise ArgumentError, 'Value given must be greater than zero.' unless (batch_size.is_a? Integer && x > 0)

    #SQL - retrieve first batch, order ASC, offset to start value, limit to batch_size
    rows = connection.execute <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE id >= #{start}
      ORDER BY id
      LIMIT #{batch_size};
    SQL

    #all records
    records = rows_to_array(rows)

    #loop through all records & yeild to block
    while records.any?
      yield records

      break if records.size < batch_size

      last_value = records.last.id
      raise "You must include the primary key." unless last_value.present?

      #get next batch, starting at last value of previous batch
      rows = connection.execute <<-SQL
        SELECT #{columns.join ","} FROM #{table}
        WHERE id > #{last_value}
        ORDER BY id
        LIMIT #{batch_size};
      SQL

      records = rows_to_array(rows)
    end
  end

  def take(num=1)
    #check that num in an integer > 0
    raise ArgumentError, 'Value given must be a number greater than zero.' unless (id.is_a? Integer && x > 0)

    if num > 1
      rows = connection.execute <<-SQL
        SELECT #{columns.join ","} FROM #{table}
        ORDER BY random()
        LIMIT #{num};
      SQL

      rows_to_array
    else
      take_one
    end
  end

  def take_one
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY random()
      LIMIT 1;
    SQL

    init_object_from_row(row)
  end

  def first
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join(",")} FROM #{table}
      ORDER BY id ASC LIMIT 1;
    SQL

    init_object_from_row(row)
  end

  def last
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY id DESC LIMIT 1;
    SQL

    init_object_from_row(row)
  end

  def all
    rows = connection.execute <<-SQL
      SELECT #{columns.join ","} FROM #{table};
    SQL

    rows_to_array(rows)
  end

  def method_missing(m, *args)
    name_arr = m.id2name.split("_")
    attribute = name_arr.last

    find_by(attribute, *args)
  end

  def where(*args)
    if args.count > 1
      expression = args.shift
      params = args
    else
      case args.first
      when String
        expression = args.first
      when Hash
        expression_hash = BlocRecord::Utility.convert_keys(args.first)
        expression = expression_hash.map { |key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}"}.join(" and ")
      end
    end

    sql = <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE #{expression};
    SQL

    rows = connection.execute(sql, params)
    rows_to_array(rows)
  end

  # Supports ordering by ASC or DESC, ordering by multiple conditions, string or symbol
  def order(*args)
    args.map!{ |arg| arg.is_a?(Hash) ? arg.map{ |k,v| "#{k} #{v.upcase}"} : arg }
    order = args.flatten(2).join(", ")

    rows = connection.execute <<-SQL
      SELECT * FROM #{table}
      ORDER BY #{order};
    SQL

    rows_to_array(rows)
  end

  def join(*args)
    if args.count > 1
      joins = args.map { |arg| "INNER JOIN #{arg} ON #{arg}.#{table}_id = #{table}.id" }.join(" ")
      rows = connection.execute <<-SQL
        SELECT * FROM #{table} #{joins}
      SQL
    else
      case args.first
      when String
        rows = connection.execute <<-SQL
          SELECT * FROM #{table} #{BlocRecord::Utility.sql_strings(args.first)};
        SQL
      when Symbol
        rows = connection.execute <<-SQL
          SELECT * FROM #{table}
          INNER JOIN #{args.first} ON #{args.first}.#{table}_id = #{table}.id
        SQL
      when Hash
        associations = args.first.flatten

        rows = connection.execute <<-SQL
          SELECT * FROM #{table}
          INNER JOIN #{associations.first} ON #{associations.first}.#{table}_id = #{table}.id
          INNER JOIN #{associations.last} ON #{associations.last}.#{associations.first}_id = #{associations.first}.id
        SQL
      else
        "Please enter valid JOINS syntax."
      end
    end

    rows_to_array(rows)
  end

  private
  def init_object_from_row(row)
    if row
      data = Hash[columns.zip(row)]
      new(data)
    end
  end

  def rows_to_array(rows)
    collection = BlocRecord::Collection.new
    rows.each { |row| collection << new(Hash[columns.zip(row)]) }
    collection
  end
end
