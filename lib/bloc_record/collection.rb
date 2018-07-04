module BlocRecord
  class Collection < Array
    def update_all(updates)
      ids = self.map(&:id)
      self.any? ? self.first.class.update(ids, updates) : false
    end

    def take
      self.first
    end

    def where(arg)
      case arg
      when String
        string_to_array = arg.split("=")
        attribute = string_to_array.first.strip
        value = string_to_array.last.strip
      when Hash
        attribute = arg.first[0].class == Symbol ? arg.first[0].id2name : arg.first[0]
        value = arg.first[1]
      end

      self.select {|result| result[attribute] == value}
    end

    def not(arg)
      self - where(arg)
    end

    def destroy_all
      self.first.class.destroy_all
    end
  end
end
