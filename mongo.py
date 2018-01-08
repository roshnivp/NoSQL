 from pymongo import MongoClient
 
 client = MongoClient("team2-vm1:27222")
 
 db = client.amazon
 reviews = db.reviews
 product = db.products
 
 
print "Query 1"
 for product in reviews.find({"overall":{"$gt":3.0}},{"_id”:0,"asin":1,"reviewerName":1}).limit(10):
   print product

print "Query 2"
 for customer in reviews.find({"asin":"0000013714"},{"reviewerName":1,"_id":0}):
   print customer


print "Query 3"
 for products in reviews.find({"reviewerID":"A23PISU0ZLW71C"},{"reviewerID":1,"reviewerName":1,"asin":1})
   print products

 

print "Query 4"
 from pprint import pprint  
 pipeline = reviews.aggregate([{"$group":{"_id":"$asin","maxrating":{"$max":{"$avg":"$overall"}}}},{"$limit":10}])
 pprint(list()pipeline)

print "Query 4"
 pipeline = [{"$group":{"_id":"$asin","count":{"$sum":1}}},{"$sort":SON([("count",1),("_id",-1)])}]
 pprint(list(reviews.aggregate(pipeline)))

print "Query 5"
 reviews.find({"helpful":{"$in":[0]}}).count()

print "Query 6"
 for review in reviews.find({"reviewerName":"GCM"},{"reviewerName":1,"reviewText":1,"_id":0,"asin":1}):
 	print review

print "Query 7"
db.review1.aggregate([{$match: {"reviewerName": {$exists: true, $ne: null}}}, {$group: { "_id":"$reviewerName", "count":{$sum:1}}}, {$sort: {"count": -1 }},{$limit:10}],{allowDiskUse:true})

print "Query 8"
pipeline = products.aggregate([{"$unwind":"$salesRank.Movies & TV"},{"$sort":{"salesRank.Movies & TV":1}},{"$group":{"_id":"$brand","salesRank":{"$push":"$salesRank"},"product":{"$push":"$asin"}}}])
pprint(list(pipeline))

print "Query 9"
pipeline = products.aggregate([{"$unwind":"$salesRank.Books"},{"$sort":{"salesRank.Books":-1}},{"$group":{"_id":"$asin","salesRank":{"$push":"$salesRank"}}},{"$limit":10}])
pprint(list(pipeline))

print "Query 10"
for brand in products.aggregate([{"$unwind":"$salesRank.Movies & TV"},{"$sort":{"salesRank.Movies & TV":-1}},{"$group":{"_id":"$brand","count":{"$sum":1}}}]):
	print brand

print "Query 11"
for product in products.find({"price":{"$gte":10,"$lte":20}},{"_id":0,"asin":1,"price":1}):
	print product

print "Query 12"
db.products.find({"categories":{"$elemMatch":{"$elemMatch":{"$eq":"Books"}}}},{"price":1,"_id":0,"categories":1,"asin":1}).sort({"price":-1}).limit(10)


print "Query 13"
db.reviews.createIndex({reviewTime:"text"})
pipeline = reviews.aggregate([{"$match":{"text":{"$search":2000}}},{"$group":{"_id":"$asin","count":{"$sum":1}}},{"$sort":{"count":-1}},{"$limit":10}])
pprint(list(pipeline))

print "Query 14"
 reviews.find({"asin":"0000013714","reviewerName":"GCM"}).count()
 
print "Query 15"
 reviews.find({"asin":"0000013714"}).count()

print "Query 16"
db.review1.aggregate([{"$group":{"_id":"$_id","helpful":{"$push":"$helpful"},"Product":{"$push":"$asin"}}},{"$sort":{"helpful":-1}},{"$limit":10}],{allowDiskUse: true})


