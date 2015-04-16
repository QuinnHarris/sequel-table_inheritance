# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
#require 'sequel/table_inheritance/version'

Gem::Specification.new do |s|
  s.name          = "sequel-table_inheritance"
  s.version       = '0.1.0' #Sequel::TableInheritance::VERSION
  s.authors       = ["Quinn Harris"]
  s.email         = ["sequel@quinnharris.me"]

  s.summary       = "Alternative to single and class table inheritance plugins for sequel"
  s.description   = s.summary
  s.homepage      = "TODO: Put your gem's website or public repo URL here."
  s.license       = "MIT"

  s.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  s.bindir        = "bin"
  s.require_paths = ["lib"]

  s.add_development_dependency "bundler", "~> 1.9"
  s.add_development_dependency "rake", "~> 10.0"
  s.add_development_dependency "rspec"

  s.add_dependency "sequel", "~> 4.19"
end
