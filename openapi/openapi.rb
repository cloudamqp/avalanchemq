require "yaml"

DEBUG = ENV.fetch("DEBUG", false)

module Helper
  module_function

  def basename(src_file)
    File.basename(src_file, ".cr")
  end

  def spec_path(src_file)
    File.join(".", "paths", "#{basename(src_file)}.yaml")
  end
end

Route = Struct.new(:route, :verb, :src_file) do
  def to_openapi
    {
      "tags" => [tag],
      "summary" => route,
      "operationId" => route,
      "responses" => {
        200 => {
          "description" => "The description",
          "content" => {
            "application/json" => {
              "schema" => {
                "$ref" => "../openapi.yaml#/components/schemas/Foo"
              }
            }
          }
        }
      }
    }
  end

  # https://swagger.io/docs/specification/describing-parameters/
  def with_path_param
    route.gsub(/:(\w+)/, '{\1}')
  end

  def spec_path
    Helper.spec_path(src_file)
  end

  def tag
    Helper.basename(src_file)
  end

  # See "Escape Characters" at https://swagger.io/docs/specification/using-ref/
  def ref
    spec_path + "#/" + with_path_param.gsub("~", "~0").gsub("/", "~1")
  end
end

openapi_spec = YAML.load(File.read(File.join(__dir__, "openapi.yaml")))
src_code_dir = File.expand_path(File.join(__dir__, ".."))
http_code_dir = File.join(src_code_dir, "src/avalanchemq/http")
files = Dir.glob(File.join(http_code_dir, "**/*.cr"))
route_regex = /(?<verb>get|post|delete)\s+"\/api(?<route>\/.+)"/i

files_with_api_routes = Hash.new { |hash, key| hash[key] = [] }

# sort so thing doesn't moved around
files.sort.each do |file|
  File.readlines(file).each do |line|
    line.match(route_regex) do |matches|
      route = Route.new(matches["route"], matches["verb"], file)

      files_with_api_routes[file] << route
    end
  end
end

# because we read the existing file
openapi_spec["paths"] = {}

# tags
existing_tags = openapi_spec.fetch("tags", [])
tag_names_from_src = files_with_api_routes.map { |src_file, _| Helper.basename(src_file) }

# keep existing tag objects
tags = existing_tags.select { |tag| tag_names_from_src.include?(tag.fetch("name")) }

# add new tags from source code
tags += tag_names_from_src.map do |tag_name|
  if tags.none? { |tag| tag.fetch("name") ==  tag_name }
    { "name" => tag_name, "description" => "placeholder" }
  end
end.compact

files_with_api_routes.each do |src_file, api_routes|
  openapi_routes = {}

  api_routes.each do |route|
    openapi_routes[route.with_path_param] = { route.verb => route.to_openapi }
    openapi_spec["paths"][route.with_path_param] = { "$ref" => route.ref }
  end

  File.write(File.join(__dir__, Helper.spec_path(src_file)), YAML.dump(openapi_routes))
end

openapi_spec["tags"] = tags

puts YAML.dump(openapi_spec) if DEBUG

File.write(File.join(__dir__, "openapi.yaml"), YAML.dump(openapi_spec))