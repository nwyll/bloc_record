module BlocRecord
  module Utility
    extend self

    def underscore(CamelCase)
      string = CamelCase.gsub(/::/,'/')
      string.gsub!(/([A-Z]+)([A-Z][a-z])/,'\1_\2')
      string.gsub!(/([a-z\d])([A-Z])/,'\1_\2')
      string.tr!("-", "_")
      string.downcase
    end

    def camelCase(snake_case)
       snake_case.split('_').collect(&:capitalize).join
    end

    def sql_strings(value)
      case value
      when String
        "'#{value}'"
      when Numeric
        value.to_s
      else
        "null"
      end
    end

    def convert_keys(options)
      options.keys.each {|key| options[key.to_s] = options.delete(key) if key.kind_of?(Symbol)}
    end

    def instance_variables_to_hash(obj)
      Hash[obj.instance_variables.map{ |var| ["#{var.to_s.delete('@')}", obj.instance_variable_get(var.to_s)]}]
    end

    def reload_obj(dirty_obj)
      persisted_obj = dirty_obj.class.find(dirty_obj.id)
      dirty_obj.instance_variables.each do |instance_variable|
        dirty_obj.instance_variable_set(instance_variable, persisted_obj.instance_variable_get(instance_variable))
      end
    end
  end
end
