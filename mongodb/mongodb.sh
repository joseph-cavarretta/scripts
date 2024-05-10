#1/bin/sh

# install couchimport
npm install -g couchimport
couchimport --version

# install mongoimport and mongoexport
wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu1804-x86_64-100.3.1.tgz
tar -xf mongodb-database-tools-ubuntu1804-x86_64-100.3.1.tgz
export PATH=$PATH:/home/project/mongodb-database-tools-ubuntu1804-x86_64-100.3.1/bin
echo "done"

# get dummy data
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/nosql/catalog.json

# import data from json file to mongo db
mongoimport -u root -p "" --authenticationDatabase admin --db catalog --collection electronics --file catalog.json

# run the following commands in mongo shell
# check that db and collection were created
show dbs
use catalog
show collections

# create index on field 'type', 1 = ascending order
db.electronics.createIndex({type:1})

# query database
db.electronics.find("type")
db.electronics.find({"type":"laptop"}).count()
db.electronics.aggregate([{$match: {"type":"smart phone"}}, {$group: {_id: "screen size", avg: {$avg:"$screen size"}}}])

# run in terminal, not mongo shell

# export specific fields from database 
mongoexport -u root -p "" --authenticationDatabase admin \
--db catalog --collection electronics --out electronics.csv --type=csv --fields _id,type,model