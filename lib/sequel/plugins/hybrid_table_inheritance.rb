module Sequel
  module Plugins
    # = Overview
    #
    # The hybrid_table_inheritance pluging allows model subclasses to be stored
    # in either the same table as the parent model or a different table with a key
    # referencing the parent table.
    # This combines the functionality of single and class (multiple) table inheritance
    # into one plugin.  This plugin uses the single_table_inheritance plugin
    # and should work as a drop in replacement for the class_table_inheritance plugins.
    # This allows introducing new tables only when needed typically for additional
    # fields or possibly referential integrity to subclassed objects.
    #
    # = Detail
    #
    # For example, with this hierarchy:
    #
    #       Employee
    #      /        \
    #   Staff     Manager
    #     |           |
    #   Cook      Executive
    #                |
    #               CEO
    #
    # the following database schema may be used (table - columns):
    #
    # employees :: id, name, kind
    # staff :: id, manager_id
    # managers :: id, num_staff
    # executives :: id, num_managers
    #
    # The hybrid_table_inheritance plugin assumes that the root table
    # (e.g. employees) has a primary key field (usually autoincrementing),
    # and all other tables have a foreign key of the same name that points
    # to the same key in their superclass's table.  In this example,
    # the employees id column is a primary key and the id column in every
    # other table is a foreign key referencing the employees id.
    #
    # In this example the employees table stores Staff model objects and the
    # executives table stores CEO model objects.
    #
    # When using the class_table_inheritance plugin, subclasses use joined
    # datasets:
    #
    #   Employee.dataset.sql
    #   # SELECT * FROM employees
    #
    #   Manager.dataset.sql
    #   # SELECT employees.id, employees.name, employees.kind,
    #   #        managers.num_staff
    #   # FROM employees
    #   # JOIN managers ON (managers.id = employees.id)
    #
    #   CEO.dataset.sql
    #   # SELECT employees.id, employees.name, employees.kind,
    #   #        managers.num_staff, executives.num_managers
    #   # FROM employees
    #   # JOIN managers ON (managers.id = employees.id)
    #   # JOIN executives ON (executives.id = managers.id)
    #   # WHERE (employees.kind IN ('CEO'))
    #
    # This allows CEO.all to return instances with all attributes
    # loaded.  The plugin overrides the deleting, inserting, and updating
    # in the model to work with multiple tables, by handling each table
    # individually.
    #
    # This plugin requires the lazy_attributes plugin and uses it to
    # return subclass specific attributes that would not be loaded
    # when calling superclass methods (since those wouldn't join
    # to the subclass tables).  For example:
    #
    #   a = Employee.all # [<#Staff>, <#Manager>, <#Executive>]
    #   a.first.values # {:id=>1, name=>'S', :kind=>'Staff'}
    #   a.first.manager_id # Loads the manager_id attribute from the database
    #
    # If you want to get all columns in a subclass instance after loading
    # via the superclass, call Model#refresh.
    #
    #   a = Employee.first
    #   a.values # {:id=>1, name=>'S', :kind=>'CEO'}
    #   a.refresh.values # {:id=>1, name=>'S', :kind=>'Executive', :num_staff=>4, :num_managers=>2}
    #
    # = Usage
    #
    #   # Use the default of storing the class name in the sti_key
    #   # column (:kind in this case)
    #   class Employee < Sequel::Model
    #     plugin :hybrid_table_inheritance, :key=>:kind
    #   end
    #
    #   # Have subclasses inherit from the appropriate class
    #   class Staff < Employee; end    # uses staff table
    #   class Cook < Staff; end        # cooks table doesn't exist so uses staff table
    #   class Manager < Employee; end  # uses managers table
    #   class Executive < Manager; end # uses executives table
    #   class CEO < Manager; end       # ceos table doesn't exist so uses executives table
    #
    #   # Some examples of using these options:
    #
    #   # Specifying the tables with a :table_map hash
    #   Employee.plugin :hybrid_table_inheritance,
    #     :table_map=>{:Employee  => :employees,
    #                  :Staff     => :staff,
    #                  :Cook      => :staff,
    #                  :Manager   => :managers,
    #                  :Executive => :executives,
    #                  :CEO       => :executives }
    #
    #   # Using integers to store the class type, with a :model_map hash
    #   # and an sti_key of :type
    #   Employee.plugin :hybrid_table_inheritance, :type,
    #     :model_map=>{1=>:Staff, 2=>:Cook, 3=>:Manager, 4=>:CEO}
    #
    #   # Using non-class name strings
    #   Employee.plugin :hybrid_table_inheritance, :key=>:type,
    #     :model_map=>{'staff'=>:Staff, 'cook staff'=>:Cook, 'supervisor'=>:Manager}
    #
    #   # By default the plugin sets the respective column value
    #   # when a new instance is created.
    #   Cook.create.type == 'cook staff'
    #   Manager.create.type == 'supervisor'
    #
    #   # You can customize this behavior with the :key_chooser option.
    #   # This is most useful when using a non-bijective mapping.
    #   Employee.plugin :hybrid_table_inheritance, :key=>:type,
    #     :model_map=>{'cook staff'=>:Cook, 'supervisor'=>:Manager},
    #     :key_chooser=>proc{|instance| instance.model.sti_key_map[instance.model.to_s].first || 'stranger' }
    #
    #   # Using custom procs, with :model_map taking column values
    #   # and yielding either a class, string, symbol, or nil,
    #   # and :key_map taking a class object and returning the column
    #   # value to use
    #   Employee.plugin :single_table_inheritance, :key=>:type,
    #     :model_map=>proc{|v| v.reverse},
    #     :key_map=>proc{|klass| klass.name.reverse}
    #
    #   # You can use the same class for multiple values.
    #   # This is mainly useful when the sti_key column contains multiple values
    #   # which are different but do not require different code.
    #   Employee.plugin :single_table_inheritance, :key=>:type,
    #     :model_map=>{'staff' => "Staff",
    #                  'manager' => "Manager",
    #                  'overpayed staff' => "Staff",
    #                  'underpayed staff' => "Staff"}
    #
    # One minor issue to note is that if you specify the <tt>:key_map</tt>
    # option as a hash, instead of having it inferred from the <tt>:model_map</tt>,
    # you should only use class name strings as keys, you should not use symbols
    # as keys.
    module HybridTableInheritance
      # The class_table_inheritance plugin requires the lazy_attributes plugin
      # to handle lazily-loaded attributes for subclass instances returned
      # by superclass methods.
      def self.apply(model, opts = OPTS)
        model.plugin :single_table_inheritance, nil
        model.plugin :lazy_attributes
      end

      # Setup the plugin using the following options:
      #  :key :: column symbol that holds the key that identifies the class to use.
      #          Necessary if you want to call model methods on a superclass
      #          that return subclass instances
      #  :model_map :: Hash or proc mapping the key column values to model class names.
      #  :key_map :: Hash or proc mapping model class names to key column values.
      #              Each value or return is an array of possible key column values.
      #  :key_chooser :: proc returning key for the provided model instance
      #  :table_map :: Hash with class name symbols keys mapping to table name symbol values
      #                Overrides implicit table names
      def self.configure(model, opts = OPTS)
        SingleTableInheritance.configure model, opts[:key], opts

        model.instance_eval do
          @cti_base_model = self
          @cti_tables = [table_name]
          @cti_columns = {table_name=>columns}
          @cti_table_map = opts[:table_map] || {}
        end
      end

      module ClassMethods
        # Hash with table name symbol keys and arrays of column symbol values,
        # giving the columns to update in each backing database table.
        attr_reader :cti_columns

        # An array of table symbols that back this model.  The first is
        # cti_base_model table symbol, and the last is the current model
        # table symbol.
        attr_reader :cti_tables

        # A hash with class name symbol keys and table name symbol values.
        # Specified with the :table_map option to the plugin, and used if
        # the implicit naming is incorrect.
        attr_reader :cti_table_map


        def inherited(subclass)
          ds = sti_dataset

          # Prevent inherited in model/base.rb from setting the dataset
          subclass.instance_eval { @dataset = nil }

          @cti_tables.push ds.first_source_alias # Kludge to change filter on cti_base_model table
          super # Call single_table_inheritance
          @cti_tables.pop

          ctm = cti_table_map
          ct = cti_tables.dup
          cc = cti_columns
          pk = primary_key

          # Set table if this is a class table inheritance
          table = nil
          columns = nil
          if (n = subclass.name) && !n.empty?
            if table = ctm[n.to_sym]
              columns = db.from(table).columns
            else
              table = subclass.implicit_table_name
              begin
                columns = db.from(table).columns
                table = nil if !columns || columns.empty?
              rescue Sequel::DatabaseError
                table = nil
              end
            end
          end
          table = nil if table && (table == table_name)

          subclass.instance_eval do
            @cti_table_map = ctm

            if table
              if ct.length == 1
                ds = ds.select(*self.columns.map{|cc| Sequel.qualify(table_name, Sequel.identifier(cc))})
              end
              @sti_dataset = ds.join(table, pk=>pk).select_append(*(columns - [pk]).map{|cc| Sequel.qualify(table, Sequel.identifier(cc))})
              set_dataset(@sti_dataset)
              set_columns(self.columns)
              dataset.row_proc = lambda{|r| subclass.sti_load(r)}

              @cti_tables = ct + [table]
              @cti_columns = cc.merge!(table=>columns)

              (columns - [pk]).each{|a| define_lazy_attribute_getter(a, :dataset=>dataset, :table=>table)}
              cti_tables.reverse.each do |ct|
                db.schema(ct).each{|sk,v| db_schema[sk] = v}
              end
            else
              @cti_tables = ct
              @cti_columns = cc
            end
          end
        end

        # The table name for the current model class's main table (not used
        # by any superclasses).
        def table_name
          cti_tables ? cti_tables.last : super
        end

        def sti_class_from_key(key)
          sti_class(sti_model_map[key])
        end
      end

      module InstanceMethods
        # Delete the row from all backing tables, starting from the
        # most recent table and going through all superclasses.
        def delete
          raise Sequel::Error, "can't delete frozen object" if frozen?
          m = model
          m.cti_tables.reverse.each do |table|
            m.db.from(table).filter(m.primary_key=>pk).delete
          end
          self
        end

        private

        # Set the sti_key column based on the sti_key_map.
        def _before_validation
          if new? && (set = self[model.sti_key])
            exp = model.sti_key_chooser.call(self)
            if set != exp
              set_table = model.sti_class_from_key(set).table_name
              exp_table = model.sti_class_from_key(exp).table_name
              set_column_value("#{model.sti_key}=", exp) if set_table != exp_table
            end
          end
          super
        end

        # Insert rows into all backing tables, using the columns
        # in each table.
        def _insert
          return super if model.cti_tables.length == 1
          iid = @values[primary_key]
          m = model
          m.cti_tables.each do |table|
            h = {}
            h[m.primary_key] ||= iid if iid
            m.cti_columns[table].each{|c| h[c] = @values[c] if @values.include?(c)}
            nid = m.db.from(table).insert(h)
            iid ||= nid
          end
          @values[primary_key] = iid
        end

        # Update rows in all backing tables, using the columns in each table.
        def _update(columns)
          pkh = pk_hash
          m = model
          m.cti_tables.each do |table|
            h = {}
            m.cti_columns[table].each{|c| h[c] = columns[c] if columns.include?(c)}
            m.db.from(table).filter(pkh).update(h) unless h.empty?
          end
        end
      end
    end
  end
end
