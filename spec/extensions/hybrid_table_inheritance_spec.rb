require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model, "hybrid table inheritance plugin" do
  before do
    @db = Sequel.mock(:autoid=>proc{|sql| 1})
    def @db.supports_schema_parsing?() true end
    def @db.schema(table, opts={})
      {:employees=>[[:id, {:primary_key=>true, :type=>:integer}], [:name, {:type=>:string}], [:kind, {:type=>:string}]],
       :managers=>[[:id, {:type=>:integer}], [:num_staff, {:type=>:integer}]],
       :uber_managers=>[[:id, {:type=>:integer}], [:special, {:type=>:string}]],
       :executives=>[[:id, {:type=>:integer}], [:num_managers, {:type=>:integer}]],
       :staff=>[[:id, {:type=>:integer}], [:manager_id, {:type=>:integer}]],
      }[table.is_a?(Sequel::Dataset) ? table.first_source_table : table]
    end
    @db.extend_datasets do
      def columns
        {[:employees]=>[:id, :name, :kind],
         [:managers]=>[:id, :num_staff],
         [:uber_managers]=>[:id, :special],
         [:executives]=>[:id, :num_managers],
         [:staff]=>[:id, :manager_id],
         [:employees, :managers]=>[:id, :name, :kind, :num_staff],
         [:employees, :managers, :uber_managers]=>[:id, :name, :kind, :num_staff, :special],
         [:employees, :managers, :executives]=>[:id, :name, :kind, :num_staff, :num_managers],
         [:employees, :staff]=>[:id, :name, :kind, :manager_id],
        }[opts[:from] + (opts[:join] || []).map{|x| x.table}]
      end
    end
    class ::Employee < Sequel::Model(@db)
      def _save_refresh; @values[:id] = 1 end
      def self.columns
        dataset.columns
      end
      plugin :hybrid_table_inheritance, :key=>:kind, :table_map=>{:Staff=>:staff}
    end
    class ::Unmanaged < Employee; end
    class ::Manager < Employee
      one_to_many :staff_members, :class=>:Staff
    end
    class ::SmartManager < Manager; end
    class ::GeniusManager < SmartManager; end
    class ::UberManager < SmartManager; end
    class ::DumbManager < Manager; end
    class ::Executive < Manager; end
    class ::Staff < Employee
      many_to_one :manager
    end
    @ds = Employee.dataset
  end

  def remove_subclasses
    Object.send(:remove_const, :Unmanaged)
    Object.send(:remove_const, :Executive)
    Object.send(:remove_const, :Manager)
    Object.send(:remove_const, :SmartManager)
    Object.send(:remove_const, :GeniusManager)
    Object.send(:remove_const, :UberManager)
    Object.send(:remove_const, :DumbManager)
    Object.send(:remove_const, :Staff)
  end

  after do
    remove_subclasses
    Object.send(:remove_const, :Employee)
  end

  def should_datasets
    Unmanaged.dataset.sql.should == "SELECT * FROM employees WHERE (employees.kind IN ('Unmanaged'))"
    Manager.dataset.sql.should == "SELECT employees.id, employees.name, employees.kind, managers.num_staff FROM employees INNER JOIN managers ON (managers.id = employees.id)"
    SmartManager.dataset.sql.should == "SELECT employees.id, employees.name, employees.kind, managers.num_staff FROM employees INNER JOIN managers ON (managers.id = employees.id) WHERE (employees.kind IN ('SmartManager', 'GeniusManager', 'UberManager'))"
    GeniusManager.dataset.sql.should == "SELECT employees.id, employees.name, employees.kind, managers.num_staff FROM employees INNER JOIN managers ON (managers.id = employees.id) WHERE (employees.kind IN ('GeniusManager'))"
    UberManager.dataset.sql.should == "SELECT employees.id, employees.name, employees.kind, managers.num_staff, uber_managers.special FROM employees INNER JOIN managers ON (managers.id = employees.id) INNER JOIN uber_managers ON (uber_managers.id = managers.id)"
    DumbManager.dataset.sql.should == "SELECT employees.id, employees.name, employees.kind, managers.num_staff FROM employees INNER JOIN managers ON (managers.id = employees.id) WHERE (employees.kind IN ('DumbManager'))"
    Executive.dataset.sql.should == "SELECT employees.id, employees.name, employees.kind, managers.num_staff, executives.num_managers FROM employees INNER JOIN managers ON (managers.id = employees.id) INNER JOIN executives ON (executives.id = managers.id)"
    Staff.dataset.sql.should == "SELECT employees.id, employees.name, employees.kind, staff.manager_id FROM employees INNER JOIN staff ON (staff.id = employees.id)"
  end

  specify "should implicity determine dataset" do
    should_datasets
  end

  specify "should explicity determine dataset" do
    table_map = {
        :Employee => :employees,
        :Unmanaged => :employees,
        :Manager => :managers,
        :SmartManager => :managers,
        :GeniusManager => :managers,
        :UberManager => :uber_managers,
        :DumbManager => :managers,
        :Executive => :executives,
        :Staff => :staff
    }
    Employee.plugin :hybrid_table_inheritance, :key=>:kind, :table_map=>table_map
    remove_subclasses
    class ::Unmanaged < Employee; end
    class ::Manager < Employee
      one_to_many :staff_members, :class=>:Staff
    end
    class ::SmartManager < Manager; end
    class ::GeniusManager < SmartManager; end
    class ::UberManager < SmartManager; end
    class ::DumbManager < Manager; end
    class ::Executive < Manager; end
    class ::Staff < Employee
      many_to_one :manager
    end

    should_datasets
  end
end