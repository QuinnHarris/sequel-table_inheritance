module Sequel
  module Plugins
    # NEEDS DOCUMENTATION
    module HybridTableInheritance
      # The class_table_inheritance plugin requires the lazy_attributes plugin
      # to handle lazily-loaded attributes for subclass instances returned
      # by superclass methods.
      def self.apply(model, *args)
        model.plugin :lazy_attributes
      end

      # Setup the necessary STI variables, see the module RDoc for SingleTableInheritance
      def self.configure(model, *args)
        key = args.shift if args.first.is_a?(Symbol)
        raise "Unexpected arguments" if args.length > 1
        opts = args.first || OPTS
        key ||= opts[:key]

        model.instance_eval do
          @cti_base_model = self
          @cti_tables = [table_name]
          @cti_columns = {table_name=>columns}
          @cti_table_map = opts[:table_map] || {}
        end

        model.instance_eval do
          @sti_key_array = nil
          @sti_key = key
          @sti_dataset = dataset
          @sti_model_map = opts[:model_map] || lambda{|v| v if v && v != ''}
          @sti_key_map = if km = opts[:key_map]
            if km.is_a?(Hash)
              h = Hash.new do |h1,k|
                unless k.is_a?(String)
                  h1[k.to_s]
                else
                  []
                end
              end
              km.each do |k,v|
                h[k.to_s] = [ ] unless h.key?(k.to_s)
                h[k.to_s].push( *Array(v) )
              end
              h
            else
              km
            end
          elsif sti_model_map.is_a?(Hash)
            h = Hash.new do |h1,k|
              unless k.is_a?(String)
                h1[k.to_s]
              else
                []
              end
            end
            sti_model_map.each do |k,v|
              h[v.to_s] = [ ] unless h.key?(v.to_s)
              h[v.to_s] << k
            end
            h
          else
            lambda{|klass| klass.name.to_s}
          end
          @sti_key_chooser = opts[:key_chooser] || lambda{|inst| Array(inst.model.sti_key_map[inst.model]).last }
          dataset.row_proc = lambda{|r| model.sti_load(r)}
        end
      end

      module ClassMethods
        # The parent/root/base model for this class table inheritance hierarchy.
        # This is the only model in the hierarchy that load the
        # class_table_inheritance plugin.
        attr_reader :cti_base_model

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


        # The base dataset for STI, to which filters are added to get
        # only the models for the specific STI subclass.
        attr_reader :sti_dataset

        # The column name holding the STI key for this model
        attr_reader :sti_key

        # Array holding keys for all subclasses of this class, used for the
        # dataset filter in subclasses. Nil in the main class.
        attr_reader :sti_key_array

        # A hash/proc with class keys and column value values, mapping
        # the class to a particular value given to the sti_key column.
        # Used to set the column value when creating objects, and for the
        # filter when retrieving objects in subclasses.
        attr_reader :sti_key_map

        # A hash/proc with column value keys and class values, mapping
        # the value of the sti_key column to the appropriate class to use.
        attr_reader :sti_model_map

        # A proc which returns the value to use for new instances.
        # This defaults to a lookup in the key map.
        attr_reader :sti_key_chooser

        # Copy the necessary attributes to the subclasses, and filter the
        # subclass's dataset based on the ti_kep_map entry for the class.
        def inherited(subclass)
          cc = cti_columns
          ctm = cti_table_map # Removed .dup
          ct = cti_tables.dup
          cbm = cti_base_model
          ds = sti_dataset


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

          if table
            pk = primary_key
            if ct.length == 1
              ds = ds.select(*self.columns.map{|cc| Sequel.qualify(table_name, Sequel.identifier(cc))})
            end
            # Need to set dataset and columns before calling super so that
            # the main column accessor module is included in the class before any
            # plugin accessor modules (such as the lazy attributes accessor module).
            subclass.instance_eval do
              @cti_base_model = cbm
              set_dataset(ds = ds.join(table, pk=>pk).select_append(*(columns - [primary_key]).map{|cc| Sequel.qualify(table, Sequel.identifier(cc))}))
              set_columns(self.columns)
            end
          end

          # Following is almost identical to inherited method in single_table_inheritance
          super
          sk = sti_key
          sd = sti_dataset
          skm = sti_key_map
          smm = sti_model_map
          skc = sti_key_chooser
          key = Array(skm[subclass]).dup
          sti_subclass_added(key)
          rp = dataset.row_proc
          subclass.set_dataset(sd.filter(SQL::QualifiedIdentifier.new(cbm.table_name, sk)=>key), :inherited=>true) unless table
          subclass.instance_eval do
            dataset.row_proc = rp
            @sti_key = sk
            @sti_key_array = key
            @sti_dataset = sd
            @sti_key_map = skm
            @sti_model_map = smm
            @sti_key_chooser = skc
            self.simple_table = nil
          end


          subclass.instance_eval do
            @cti_table_map = ctm
            @cti_base_model = cbm

            if table
              dataset.row_proc = lambda{|r| subclass.sti_load(r)}
              @cti_tables = ct + [table]
              @cti_columns = cc.merge(table=>columns)
              @sti_dataset = ds

              (columns - [cbm.primary_key]).each{|a| define_lazy_attribute_getter(a, :dataset=>dataset, :table=>table)}
              cti_tables.reverse.each do |ct|
                db.schema(ct).each{|sk,v| db_schema[sk] = v}
              end
            else
              @cti_tables = ct
              @cti_columns = cc
            end
          end
        end

        # The primary key in the parent/base/root model, which should have a
        # foreign key with the same name referencing it in each model subclass.
        def primary_key
          return super if self == cti_base_model
          cti_base_model.primary_key
        end

        # The table name for the current model class's main table (not used
        # by any superclasses).
        def table_name
          self == cti_base_model ? super : cti_tables.last
        end

        # Return an instance of the class specified by sti_key,
        # used by the row_proc.
        def sti_load(r)
          sti_class(sti_model_map[r[sti_key]]).call(r)
        end

        def sti_class_from_key(key)
          sti_class(sti_model_map[key])
        end

        # Make sure that all subclasses of the parent class correctly include
        # keys for all of their descendant classes.
        def sti_subclass_added(key)
          if sti_key_array
            key_array = Array(key)
            Sequel.synchronize{sti_key_array.push(*key_array)}
            superclass.sti_subclass_added(key)
          end
        end

        private

        # If calling set_dataset manually, make sure to set the dataset
        # row proc to one that handles inheritance correctly.
        def set_dataset_row_proc(ds)
          ds.row_proc = @dataset.row_proc if @dataset
        end

        # Return a class object.  If a class is given, return it directly.
        # Treat strings and symbols as class names.  If nil is given or
        # an invalid class name string or symbol is used, return self.
        # Raise an error for other types.
        def sti_class(v)
          case v
          when String, Symbol
            constantize(v) rescue self
          when nil
            self
          when Class
            v
          else
            raise(Error, "Invalid class type used: #{v.inspect}")
          end
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
          if new? && model.sti_key #!self[model.sti_key]
            exp = model.sti_key_chooser.call(self)
            if (set = self[model.sti_key]) && (set != exp)
              set_table = model.sti_class_from_key(set).table_name
              exp_table = model.sti_class_from_key(exp).table_name
              exp = set if set_table == exp_table
            end
            set_column_value("#{model.sti_key}=", exp)
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
