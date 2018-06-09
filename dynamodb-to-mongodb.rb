require 'aws-sdk'
require 'mongo'
require 'csv'
class Dynamodb
  def initialize
    @client = Mongo::Client.new('mongodb://127.0.0.1:27017/test') #mongodb connection
    @collection = @client[:dynamodb]
    Aws.config.update({
      region: "region here",
      endpoint: "dynamodb end point here",
      access_key_id: "access key here",
      secret_access_key: "secret access key here"
    })
    @dynamodb = Aws::DynamoDB::Client.new #dynamodb connection
  end

  attr_reader :client, :collection, :dynamodb

  def execute
    begin
      params = {
        table_name: "table name here"
        #total_segments: 20,
        #segment: 2
      }
      response = dynamodb.scan(params)
      i = 1
      loop do
        # when the result doesn't have next page
        break unless (lek = response.last_evaluated_key)
        items = response.items
        items.each do |record|
          data = {}
          data['dynamodb_id'] = record['id']
          data['name'] = record['name']
          begin
            collection.insert_one(data)
            puts "#{i}==#{data['dynamodb_id']}"
            puts "=" * 50
          rescue Exception => e
            puts e.message
            CSV.open("./dynamodb_fail.csv", "wb", :write_headers=>true,:headers=>['dynamodb_id','error']) do |csv|
              csv << [data['dynamodb_id'], e.message]
            end
          end
          i += 1
        end
        response = dynamodb.scan(params.merge(exclusive_start_key: lek))
      end
    rescue Exception => e
      puts e.message
    end
  end
end
obj = Dynamodb.new
obj.execute

