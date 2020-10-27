require "../spec_helper"

describe AvalancheMQ::HTTP::ParametersController do
  describe "GET /api/parameters" do
    it "should return all vhost scoped parameters for policymaker" do
      s.users.create("arnold", "pw", [AvalancheMQ::Tag::PolicyMaker])
      s.users.add_permission("arnold", "/", /.*/, /.*/, /.*/)
      hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      p = AvalancheMQ::Parameter.new("test", "name", JSON::Any.new({} of String => JSON::Any))
      s.vhosts["/"].add_parameter(p)
      response = get("/api/parameters", headers: hdrs)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["name", "component", "vhost", "value"]
      body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    ensure
      s.vhosts["/"].delete_parameter("test", "name")
      s.users.delete("arnold")
    end

    it "should refuse monitoring and management" do
      s.users.create("arnold", "pw", [AvalancheMQ::Tag::Management, AvalancheMQ::Tag::Monitoring])
      s.users.rm_permission("arnold", "/")
      hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      response = get("/api/parameters", headers: hdrs)
      response.status_code.should eq 401
    ensure
      s.users.delete("arnold")
    end
  end

  describe "GET /api/parameters/component" do
    it "should return all parameters for a component" do
      p = AvalancheMQ::Parameter.new("test", "name", JSON::Any.new({} of String => JSON::Any))
      s.vhosts["/"].add_parameter(p)
      response = get("/api/parameters/test")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      s.vhosts["/"].delete_parameter("test", "name")
    end
  end

  describe "GET /api/parameters/component/vhost" do
    it "should return all parameters for a component on vhost" do
      p = AvalancheMQ::Parameter.new("test", "name", JSON::Any.new({} of String => JSON::Any))
      s.vhosts["/"].add_parameter(p)
      response = get("/api/parameters/test/%2f")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      s.vhosts["/"].delete_parameter("test", "name")
    end
  end

  describe "GET /api/parameters/component/vhost/name" do
    it "should return parameter" do
      p = AvalancheMQ::Parameter.new("test", "name", JSON::Any.new({} of String => JSON::Any))
      s.vhosts["/"].add_parameter(p)
      response = get("/api/parameters/test/%2f/name")
      response.status_code.should eq 200
    ensure
      s.vhosts["/"].delete_parameter("test", "name")
    end
  end

  describe "PUT /api/parameters/component/vhost/name" do
    it "should create parameters for a component on vhost" do
      body = %({
        "value": {}
      })
      response = put("/api/parameters/test/%2f/name", body: body)
      response.status_code.should eq 204
    ensure
      s.vhosts["/"].delete_parameter("test", "name")
    end

    it "should handle request with empty body" do
      response = put("/api/parameters/test/%2f/name", body: "")
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should match(/Field .+ is required/)
    end
  end

  describe "DELETE /api/parameters/component/vhost/name" do
    it "should delete parameter for a component on vhost" do
      p = AvalancheMQ::Parameter.new("test", "name", JSON::Any.new({} of String => JSON::Any))
      s.vhosts["/"].add_parameter(p)
      response = delete("/api/parameters/test/%2f/name")
      response.status_code.should eq 204
    ensure
      s.vhosts["/"].delete_parameter("test", "name")
    end
  end

  describe "GET /api/global-parameters" do
    it "should return all global parameters" do
      p = AvalancheMQ::Parameter.new(nil, "name", JSON::Any.new({} of String => JSON::Any))
      s.add_parameter(p)
      response = get("/api/global-parameters")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["name", "value"]
      body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    ensure
      s.delete_parameter(nil, "name")
    end
  end

  describe "GET /api/global-parameters/name" do
    it "should return parameter" do
      p = AvalancheMQ::Parameter.new(nil, "name", JSON::Any.new({} of String => JSON::Any))
      s.add_parameter(p)
      response = get("/api/global-parameters/name")
      response.status_code.should eq 200
    ensure
      s.delete_parameter(nil, "name")
    end
  end

  describe "PUT /api/global-parameters/name" do
    it "should create global parameter" do
      s.delete_parameter(nil, "name")
      body = %({
        "value": {}
      })
      response = put("/api/global-parameters/name", body: body)
      response.status_code.should eq 204
    ensure
      s.delete_parameter(nil, "name")
    end

    it "should handle request with empty body" do
      response = put("/api/global-parameters/name", body: "")
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should match(/Field .+ is required/)
    end
  end

  describe "DELETE /api/global-parameters/name" do
    it "should delete parameter" do
      p = AvalancheMQ::Parameter.new(nil, "name", JSON::Any.new({} of String => JSON::Any))
      s.add_parameter(p)
      response = delete("/api/global-parameters/name")
      response.status_code.should eq 204
    ensure
      s.delete_parameter(nil, "name")
    end
  end

  describe "GET /api/policies" do
    it "should return all policies" do
      definitions = {
        "max-length"         => JSON::Any.new(10_i64),
        "alternate-exchange" => JSON::Any.new("dead-letters"),
      }
      s.vhosts["/"].add_policy("test", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, -10_i8)
      response = get("/api/policies")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["name", "vhost", "definition", "priority", "apply-to"]
      body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    ensure
      s.vhosts["/"].delete_policy("test")
    end
  end

  describe "GET /api/policies/vhost" do
    it "should return policies for vhost" do
      definitions = {
        "max-length"         => JSON::Any.new(10_i64),
        "alternate-exchange" => JSON::Any.new("dead-letters"),
      }
      s.vhosts["/"].add_policy("test", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, -10_i8)
      response = get("/api/policies/%2f")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      s.vhosts["/"].delete_policy("test")
    end
  end

  describe "GET /api/policies/vhost/name" do
    it "should return policy" do
      definitions = {
        "max-length"         => JSON::Any.new(10_i64),
        "alternate-exchange" => JSON::Any.new("dead-letters"),
      }
      s.vhosts["/"].add_policy("test", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, -10_i8)
      response = get("/api/policies/%2f/test")
      response.status_code.should eq 200
    ensure
      s.vhosts["/"].delete_policy("test")
    end
  end

  describe "PUT /api/policies/vhost/name" do
    it "should create policy" do
      body = %({
        "apply-to": "queues",
        "priority": 4,
        "definition": { "max-length": 10 },
        "pattern": ".*"
      })
      response = put("/api/policies/%2f/name", body: body)
      response.status_code.should eq 204
    ensure
      s.vhosts["/"].delete_policy("name")
    end

    it "should handle request with empty body" do
      response = put("/api/policies/%2f/name", body: "")
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should match(/Fields .+ are required/)
    end
  end

  describe "DELETE /api/policies/vhost/name" do
    it "should delete policy" do
      definitions = {
        "max-length"         => JSON::Any.new(10_i64),
        "alternate-exchange" => JSON::Any.new("dead-letters"),
      }
      s.vhosts["/"].add_policy("test", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, -10_i8)
      response = delete("/api/policies/%2f/test")
      response.status_code.should eq 204
    ensure
      s.vhosts["/"].delete_policy("test")
    end
  end
end
