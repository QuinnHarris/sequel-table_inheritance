module Sequel
  module Plugins
    # NEEDS DOCUMENTATION
    module HybridTableInheritance
      # The class_table_inheritance plugin requires the lazy_attributes plugin
      # to handle lazily-loaded attributes for subclass instances returned
      # by superclass methods.
      def self.apply(model, opts = OPTS)
        model.plugin :single_table_inheritance, nil
        model.plugin :lazy_attributes
      end

      # Setup the necessary STI variables, see the module RDoc for SingleTableInheritance
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

          @cti_tables.push cbm.table_name # Kludge to change filter on cti_base_model table
          super # Call single_table_inheritance
          @cti_tables.pop


          subclass.instance_eval do
            @cti_table_map = ctm
            @cti_base_model = cbm

            if table
              set_dataset(ds) # Don't use dataset from sti plugin
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
